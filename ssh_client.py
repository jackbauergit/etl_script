# -*- coding: utf-8 -*-


from paramiko import SSHClient
from scp import SCPClient
import paramiko
import logging
from logger import logger_mongodb_etl as logger


logging.getLogger("paramiko").setLevel(logging.WARNING)


max_try_times = 2
success_tag = '[1]-(I)-<l>-[o}-{O]-<0>-(p)-[P]'
important_dirs = ['/', '/home/']
exclude_msg = (
    'Using a password on the command line interface can be insecure')


class SSHExecutor(object):
    def __init__(self, ip, account, password):
        self.ip = ip
        self._client = SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for once in range(max_try_times):
            try:
                self._client.connect(
                    ip, username=account, password=password,
                    timeout=2)

            except:
                continue

            else:
                return

        logger.error((
            u'无法使用 %s:%s 建立到 %s 的SSH连接，请确认服务器网络连通'
            u'并且使用了正确的SSH密码！') % (account, password, ip))
        raise Exception((
            u'无法建立到 %s 的SSH连接，请确认服务器网络连通'
            u'并且使用了正确的SSH密码！') % ip)

    def __del__(self):
        if self._client:
            self._client.close()

    def get_known_host_rsa(self):
        sk = self._client.get_transport().get_remote_server_key()
        if not sk:
            logger.error(u'加载host_key失败！')

        return 'ssh-rsa %s' % sk.get_base64()

    # 执行系统命令
    def execute_in_backupground(self, cmd):
        self.naked_execute('%s >> nohup 2>&1 &' % cmd)

    def naked_execute(self, cmd, expect_input=''):
        logger.debug(u'在 %s 上直接执行: %s' % (self.ip, cmd))
        exec_cmd = cmd
        (stdin, stdout, stderr) = self._client.exec_command(exec_cmd)

        if expect_input:
            stdin.write('%s\n' % expect_input)
            stdin.flush()

        result = (''.join(stdout.readlines())).strip()
        err_infos = [
            line.decode('utf-8') for line in stderr.readlines()
            if exclude_msg not in line]
        logger.debug(result)

        if err_infos:
            raise Exception(u'\n'.join(err_infos))

        return result

    def execute(self, cmd, expect_input=None):
        logger.debug(u'在 %s 上执行: %s' % (self.ip, cmd))
        exec_cmd = "%s && echo '%s'" % (cmd, success_tag)
        (stdin, stdout, stderr) = self._client.exec_command(exec_cmd)

        if expect_input is not None:
            stdin.write('%s\n' % expect_input)
            stdin.flush()
            logger.debug(u'在 %s 上输入: %s' % (self.ip, expect_input))

        return_rows = stdout.readlines()
        err_infos = list()
        for line in stderr.readlines():
            logger.debug(line)
            err_infos.append(line)

        if err_infos or not return_rows or\
                return_rows[-1].strip() != success_tag:
            meaning_rows = return_rows[:-1] if return_rows else []
            meaning_rows = [row.decode('utf-8') for row in meaning_rows]

            err_msg = u'\n'.join(err_infos)

            err_msg = err_msg if err_msg else u''.join(meaning_rows)
            raise Exception(err_msg)

        result = (''.join(return_rows[:-1])).strip()
        logger.debug(result)
        return result

    def upload(self, source_file, dest_file):
        logger.debug(u'准备将本地文件:%s 上传至 %s:%s' % (
            source_file, self.ip, dest_file))

        with SCPClient(self._client.get_transport()) as scp:
            scp.put(source_file, dest_file)

    def has_record(self, query_cmd):
        cmd = '%s| wc -l' % query_cmd
        r = self.execute(cmd)
        if r == '0':
            return False

        return True

    def has_file(self, file_name):
        cmd = "ls %s 2>/etc/null" % file_name
        return self.has_record(cmd)

    def rm_file(self, dir_path):
        if not self.has_file(dir_path):
            return

        if dir_path in important_dirs:
            raise Exception(u'危险！不能删除目录: %s' % dir_path)

        cmd = '/bin/rm -rf %s' % dir_path
        self.execute(cmd)

    def mkdir(self, dir_path, mode=None):
        if self.has_file(dir_path):
            if mode:
                cmd = 'chmod -R %s %s' % (mode, dir_path)
                self.execute(cmd)
            return

        dir_mod = ''
        if mode:
            dir_mod = '-m %s ' % mode
        cmd = 'mkdir -p %s%s' % (dir_mod, dir_path)
        self.execute(cmd)
