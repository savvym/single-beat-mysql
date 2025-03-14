import codecs
import functools
import json
import os
import sys
import time
import socket
import logging
import signal
import asyncio
import pymysql
import aiomysql
from urllib.parse import urlparse


def noop(i):
    return i


def env(identifier, default, type=noop):
    return type(os.getenv("SINGLE_BEAT_%s" % identifier, default))


class Config(object):
    MYSQL_URL = env('MYSQL_URL', 'mysql://root:zhd961024@127.0.0.1:3306/chore_task')
    IDENTIFIER = env('IDENTIFIER', None)
    LOCK_TIME = env('LOCK_TIME', 5, int)
    INITIAL_LOCK_TIME = env('INITIAL_LOCK_TIME', LOCK_TIME * 2, int)
    HEARTBEAT_INTERVAL = env('HEARTBEAT_INTERVAL', 1, int)
    HOST_IDENTIFIER = env('HOST_IDENTIFIER', socket.gethostname())
    LOG_LEVEL = env('LOG_LEVEL', 'warn')
    # wait_mode can be, supervisord or heartbeat
    WAIT_MODE = env("WAIT_MODE", "heartbeat")
    WAIT_BEFORE_DIE = env("WAIT_BEFORE_DIE", 60, int)
    _host_identifier = None
    _mysql_config = None

    def check(self, cond, message):
        if not cond:
            raise Exception(message)

    def checks(self):
        self.check(
            self.LOCK_TIME < self.INITIAL_LOCK_TIME,
            "initial lock time must be greater than lock time",
        )
        self.check(
            self.HEARTBEAT_INTERVAL < (self.LOCK_TIME / 2.0),
            "SINGLE_BEAT_HEARTBEAT_INTERVAL must be smaller than SINGLE_BEAT_LOCK_TIME / 2",
        )
        self.check(self.WAIT_MODE in ("supervised", "heartbeat"), "undefined wait mode")
        
        # 检查MySQL连接
        conn = self.get_mysql_connection()
        conn.ping()
        conn.close()

    def parse_mysql_url(self):
        """解析MySQL URL并返回连接参数"""
        if not self._mysql_config:
            parsed = urlparse(self.MYSQL_URL)
            self._mysql_config = {
                'host': parsed.hostname or '127.0.0.1',
                'port': parsed.port or 3306,
                'user': parsed.username or 'root',
                'password': parsed.password or '',
                'db': parsed.path.strip('/') or 'chore_task',
            }
        return self._mysql_config

    def get_mysql_connection(self):
        """获取MySQL连接"""
        config = self.parse_mysql_url()
        return pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['db'],
            cursorclass=pymysql.cursors.DictCursor
        )

    def ensure_table_exists(self):
        """确保SingleBeat表存在"""
        conn = self.get_mysql_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS SingleBeat (
                    `key` VARCHAR(255) PRIMARY KEY,
                    `data` TEXT,
                    `expire_at` TIMESTAMP NULL
                )
                """)
                conn.commit()
        finally:
            conn.close()

    async def get_async_mysql_pool(self):
        """获取异步MySQL连接池"""
        config = self.parse_mysql_url()
        return await aiomysql.create_pool(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            db=config['db'],
            autocommit=True,
        )

    def get_host_identifier(self):
        """\
        we try to return IPADDR:PID form to identify where any singlebeat instance is
        running.

        :return:
        """
        if self._host_identifier:
            return self._host_identifier
        
        # 获取本地IP地址
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # 不需要真正连接
            s.connect((self.parse_mysql_url()['host'], 1))
            local_ip_addr = s.getsockname()[0]
        except:
            local_ip_addr = '127.0.0.1'
        finally:
            s.close()
            
        self._host_identifier = "{}:{}".format(local_ip_addr, os.getpid())
        return self._host_identifier


config = Config()
config.ensure_table_exists()
config.checks()

numeric_log_level = getattr(logging, config.LOG_LEVEL.upper(), None)
logging.basicConfig(level=numeric_log_level)
logger = logging.getLogger(__name__)


def get_process_identifier(args):
    """by looking at arguments we try to generate a proper identifier
    >>> get_process_identifier(['python', 'echo.py', '1'])
    'python_echo.py_1'
    """
    return "_".join(args)


class State:
    PAUSED = "PAUSED"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    RESTARTING = "RESTARTING"


def is_process_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except:
        return False


class Process(object):
    def __init__(self, args):
        self.args = args
        self.state = None
        self.t1 = time.time()

        self.identifier = config.IDENTIFIER or get_process_identifier(self.args[1:])
        self.ioloop = asyncio.get_running_loop()
        
        for signame in {"SIGINT", "SIGTERM"}:
            sig = getattr(signal, signame)
            self.ioloop.add_signal_handler(
                sig, functools.partial(self.sigterm_handler, sig, self.ioloop)
            )

        self.mysql_pool = None
        self.command_queue = asyncio.Queue()
        self.fence_token = 0
        self.sprocess = None
        self.pc = None
        self.state = State.WAITING
        self._periodic_callback_running = True
        self.child_exit_cb = self.proc_exit_cb
        self.last_command_check = 0
        self.command_check_interval = 1  # 每秒检查一次命令

    def proc_exit_cb(self, exit_status):
        """When child exits we use the same exit status code"""
        self._periodic_callback_running = False
        sys.exit(exit_status)

    def proc_exit_cb_noop(self, exit_status):
        """\
        when we deliberately restart/stop the child process,
        we don't want to exit ourselves, so we replace proc_exit_cb
        with a noop one when restarting
        :param exit_status:
        :return:
        """

    def proc_exit_cb_restart(self, exit_status):
        """\
        this is used when we restart the process,
        it re-triggers the start
        """
        self.ioloop.run_until_complete(self.spawn_process())

    def proc_exit_cb_state_set(self, exit_status):
        if self.state == State.PAUSED:
            self.state = State.WAITING

    def stdout_read_cb(self, data):
        sys.stdout.write(data)

    def stderr_read_cb(self, data):
        sys.stderr.write(data)

    async def timer_cb_paused(self):
        pass

    async def timer_cb_waiting(self):
        if await self.acquire_lock():
            logger.info(f"acquired lock, {self.identifier} spawning child process")
            return self.ioloop.create_task(self.spawn_process())
        # couldn't acquire lock
        if config.WAIT_MODE == "supervised":
            logger.debug(
                "already running, will exit after %s seconds" % config.WAIT_BEFORE_DIE
            )
            time.sleep(config.WAIT_BEFORE_DIE)
            sys.exit()

    def process_pid(self):
        """\
        when we are restarting, we want to keep sending heart beat, so any other single-beat
        node will not pick it up.
        hence we need a process-id as an identifier - even for a short period of time.
        :return:
        """
        if self.sprocess:
            return self.sprocess.pid
        return -1

    async def timer_cb_running(self):
        if not self.mysql_pool:
            self.mysql_pool = await config.get_async_mysql_pool()
            
        # 读取当前fence token
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT `data` FROM SingleBeat WHERE `key` = %s", 
                                   [f"SINGLE_BEAT_{self.identifier}"])
                row = await cursor.fetchone()
                
                if row:
                    redis_fence_token = int(row['data'].split(":")[0])
                else:
                    logger.error(
                        "fence token could not be read from MySQL - assuming lock expired, trying to reacquire lock"
                    )
                    if await self.acquire_lock():
                        logger.info("reacquired lock")
                        redis_fence_token = self.fence_token
                    else:
                        logger.error("unable to reacquire lock, terminating")
                        os.kill(os.getpid(), signal.SIGTERM)

                logger.debug(
                    "expected fence token: {} fence token read from MySQL: {}".format(
                        self.fence_token, redis_fence_token
                    )
                )

                if self.fence_token == redis_fence_token:
                    self.fence_token += 1
                    expire_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + config.LOCK_TIME))
                    await cursor.execute(
                        "UPDATE SingleBeat SET `data` = %s, `expire_at` = %s WHERE `key` = %s",
                        [
                            "{}:{}:{}".format(
                                self.fence_token, config.HOST_IDENTIFIER, self.process_pid()
                            ),
                            expire_time,
                            f"SINGLE_BEAT_{self.identifier}"
                        ]
                    )
                else:
                    logger.error(
                        "fence token did not match - lock is held by another process, terminating"
                    )
                    # send sigterm to ourself and let the sigterm_handler do the rest
                    os.kill(os.getpid(), signal.SIGTERM)

    async def timer_cb_restarting(self):
        """\
        when restarting we are doing exactly the same as running - we don't want any other
        single-beat node to pick up
        :return:
        """
        await self.timer_cb_running()

    async def timer_cb(self):
        logger.debug("timer called %s state=%s", time.time() - self.t1, self.state)
        self.t1 = time.time()
        fn = getattr(self, "timer_cb_{}".format(self.state.lower()))
        await fn()
        
        # 检查是否有命令需要执行
        await self.check_commands()

    async def acquire_lock(self):
        if not self.mysql_pool:
            self.mysql_pool = await config.get_async_mysql_pool()
            
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # 先清理过期的锁
                await cursor.execute("DELETE FROM SingleBeat WHERE `key` = %s AND `expire_at` < NOW()", 
                                   [f"SINGLE_BEAT_{self.identifier}"])
                
                # 尝试获取锁
                expire_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + config.INITIAL_LOCK_TIME))
                await cursor.execute(
                    "INSERT IGNORE INTO SingleBeat (`key`, `data`, `expire_at`) VALUES (%s, %s, %s)",
                    [
                        f"SINGLE_BEAT_{self.identifier}", 
                        "{}:{}:{}".format(self.fence_token, config.HOST_IDENTIFIER, 0),
                        expire_time
                    ]
                )
                
                # 确认是否获取到锁
                return cursor.rowcount > 0

    def sigterm_handler(self, signum, loop):
        """When we get term signal
        if we are waiting and got a sigterm, we just exit.
        if we have a child running, we pass the signal first to the child
        then we exit.

        To exit we signal our main sleep/trigger loop on `self.run()`

        :param signum:
        :param ioloop:
        :return:
        """
        assert self.state in ("WAITING", "RUNNING", "PAUSED")
        logger.debug("our state %s", self.state)
        if self.state == "WAITING":
            self._periodic_callback_running = False

        if self.state == "RUNNING":
            logger.debug(
                "already running sending signal to child - %s", self.sprocess.pid
            )
            self.sprocess.send_signal(signum)
            logger.debug("waiting for subprocess to finish")
            self.ioloop.create_task(self.sprocess.wait())
        self._periodic_callback_running = False

    async def run(self):
        self.mysql_pool = await config.get_async_mysql_pool()
        while self._periodic_callback_running:
            await self.timer_cb()
            await asyncio.sleep(config.HEARTBEAT_INTERVAL)

    async def _read_stream(self, stream, cb):
        decoder = codecs.getincrementaldecoder('utf-8')(errors='strict')

        while True:
            line = await stream.read(100)
            if line:
                cb(decoder.decode(line))
            else:
                break

    async def spawn_process(self):
        cmd = self.args
        env = os.environ

        self.state = State.RUNNING
        try:
            self.sprocess = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
        except FileNotFoundError:
            """
            if the file that we need to run doesn't exists
            we immediately exit.
            """
            logger.exception("file not found")
            return self.child_exit_cb(1)
        try:
            await asyncio.wait(
                [
                    asyncio.create_task(self._read_stream(self.sprocess.stdout, self.forward_stdout)),
                    asyncio.create_task(self._read_stream(self.sprocess.stderr, self.forward_stderr)),
                ]
            )
            self.child_exit_cb(self.sprocess.returncode)
        except SystemExit as e:
            os._exit(e.code)

    def cli_command_info(self, msg):
        info = ""
        if self.sprocess:
            if is_process_alive(self.sprocess.pid):
                info = "pid: {}".format(self.sprocess.pid)
        return info

    def child_process_alive(self):
        return not self.sprocess.protocol._process_exited

    def cli_command_quit(self, msg):
        """\
        kills the child and exits
        """
        if self.state == State.RUNNING and self.sprocess and self.child_process_alive():
            self.sprocess.kill()
        else:
            sys.exit(0)

    def cli_command_pause(self, msg):
        """\
        if we have a running child we kill it and set our state to paused
        if we don't have a running child, we set our state to paused
        this will pause all the nodes in single-beat cluster

        its useful when you deploy some code and don't want your child to spawn
        randomly

        :param msg:
        :return:
        """
        info = ""
        if self.state == State.RUNNING and self.sprocess and self.child_process_alive():
            self.child_exit_cb = self.proc_exit_cb_noop
            self.sprocess.kill()
            info = "killed"
        self.state = State.PAUSED
        return info

    def cli_command_resume(self, msg):
        """\
        sets state to waiting - so we resume spawning children
        """
        if self.state == State.PAUSED:
            self.state = State.WAITING

    def cli_command_stop(self, msg):
        """\
        stops the running child process - if its running
        it will re-spawn in any single-beat node after sometime

        :param msg:
        :return:
        """
        info = ""
        if self.state == State.RUNNING and self.sprocess and self.sprocess.proc:
            self.state = State.PAUSED
            self.sprocess.kill()
            info = "killed"
        return info

    def cli_command_restart(self, msg):
        """\
        restart the subprocess
        i. we set our state to RESTARTING - on restarting we still send heartbeat
        ii. we kill the subprocess
        iii. we start again
        iv. if its started we set our state to RUNNING, else we set it to WAITING

        :param msg:
        :return:
        """
        info = ""
        if self.state == State.RUNNING and self.sprocess and self.sprocess.proc:
            self.state = State.RESTARTING
            self.child_exit_cb = self.proc_exit_cb_restart
            self.sprocess.kill()
            info = "killed"
        return info
    
    async def check_commands(self):
        """检查MySQL中是否有命令需要执行"""
        # 每秒只检查一次命令，避免过多查询
        current_time = time.time()
        if current_time - self.last_command_check < self.command_check_interval:
            return
            
        self.last_command_check = current_time
        
        if not self.mysql_pool:
            self.mysql_pool = await config.get_async_mysql_pool()
            
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # 查询命令
                await cursor.execute(
                    "SELECT `data` FROM SingleBeat WHERE `key` = %s",
                    [f"SB_{self.identifier}"]
                )
                row = await cursor.fetchone()
                
                if row and row['data']:
                    # 处理命令
                    await self.pubsub_callback(row['data'])
                    # 处理完后清除命令
                    await cursor.execute(
                        "DELETE FROM SingleBeat WHERE `key` = %s",
                        [f"SB_{self.identifier}"]
                    )

    async def pubsub_callback(self, data_str):
        logger.info("got command - %s", data_str)
        
        try:
            cmd = json.loads(data_str)
        except:
            logger.exception("exception on parsing command %s", data_str)
            return

        fn = getattr(self, "cli_command_{}".format(cmd["cmd"]), None)
        if not fn:
            logger.info("cli_command_{} not found".format(cmd["cmd"]))
            return

        logger.info("got command - %s running %s", data_str, fn)
        info = fn(cmd)
        
        if 'reply_channel' in cmd:
            async with self.mysql_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO SingleBeat (`key`, `data`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                        [
                            cmd["reply_channel"],
                            json.dumps({
                                "identifier": config.get_host_identifier(),
                                "state": self.state,
                                "info": info or "",
                            })
                        ]
                    )

    def forward_stdout(self, buf):
        self.stdout_read_cb(buf)

    def forward_stderr(self, buf):
        self.stderr_read_cb(buf)


async def run_process():
    process = Process(sys.argv[1:])
    await process.run()


def main():
    asyncio.run(run_process())


if __name__ == "__main__":
    main()

