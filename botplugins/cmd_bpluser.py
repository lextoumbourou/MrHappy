from botplugin import BotPlugin
from fabric.api import *
from paramiko import SSHConfig
from collections import namedtuple
import logging
import re

CheckFailed = Exception('CheckFailed')
UserAccount = namedtuple('UserAccount', 'environment username email full_name')

class BPLUser(BotPlugin):

    config_options = {
        # This configurs the "example" environment. In your bot config
        # file define as many environments as you wish, using this format.
        # server: jump host name as defined in your .ssh/config
        # dbaddr: address mysql client connects to from server. eg. localhost
        # dbname: mysql database name
        # dbuser: mysql user
        # dbpass: mysql password
        'environment_example': 'server,dbaddr,dbname,dbuser,dbpass',
    }

    def setup(self, options):
        self.config = {}
        for k, v in options.items():
            self.config[k] = v

    def command_bpluser(self, bot, e, command, args, channel, nick):
        try:
            (subcmd, args) = args.split(' ', 1)
        except:
            (subcmd, args) = args, None
        if subcmd == "exists":
            self.handle_bpluser_exists(bot, args, channel, nick)
        if subcmd == "create":
            self.handle_bpluser_create(bot, args, channel, nick)

    def _user_account_from_args(args):
        """
        Attempt to create a UserAccount object by splitting args

        Two or four arguments may be passed, in order:
        environment
        username
        email
        full_name (which may be multiple words)
        """
        try:
            a = args.split(' ', 3)
        except:
            return None
        if len(a) == 2:
            a.extend((None, None))
            useraccount = UserAccount(a)
        elif len(a) == 4:
            useraccount = UserAccount(a)
        else:
            return None

    def handle_bpluser_exists(self, bot, args, channel, nick):
        logging.debug('handling bpluser exists')
        ua = _user_account_from_args(args)
        if not ua:
            bot.reply('args: exists <environment> <username>', channel, nick)
            return

        if not re.match('^[a-zA-Z\d]+$', ua.username):
            bot.reply('Does not appear to be a valid username: %s' % ua.username, channel, nick)
            return

        if not self.config.has_key('environment_%s' % ua.environment):
            bot.reply('Unknown environment: %s' % ua.environment, channel, nick)
            return

        (host, dbaddr, dbname, dbuser, dbpass) = self.config['environment_%s' % ua.environment].split(',')
        dbname = self.config['dbname']

        h_info = ssh_config.lookup(host)
        try:
            h = '%s@%s:%s' % (h_info['user'], h_info['hostname'], h_info['port'])
        except:
            bot.reply('Incomplete ssh config for environment', channel, nick)
            return

        exists = check_if_user_exists(h, dbaddr, dbname, dbuser, dbpass, ua.username)
        if exists is None:
            bot.reply('Error while running check', channel, nick)
        elif exists:
            bot.reply('User %s exists in %s' % (ua.username, ua.environment), channel, nick)
        else:
            bot.reply('User %s does not exist in %s' % (ua.username, ua.environment), channel, nick)

def create_sql_query_does_user_exist(dbname, username):
    sqlfile = 'does_user_exist_%s.sql' % username
    f = open(sqlfile, 'w')
    f.write("use %s;\nselect count(*) as user_count from users where username = '%s'\\G" % (dbname, username))
    f.close()
    return sqlfile

def user_exists(dbaddr, dbname, dbuser, dbpass, user):
    sqlfile = create_sql_query_does_user_exist(dbname, user)
    put(sqlfile, sqlfile)
    result = run('mysql -h %s -u %s --password=%s < %s' % (dbaddr, dbuser, dbpass, sqlfile))
    run('rm %s' % sqlfile)
    local('rm %s' % sqlfile)
    m = re.search('user_count: (\d+)', result)
    if not m:
        raise CheckFailed
    return int(m.groups()[0])

def check_if_user_exists(host, dbaddr, dbname, dbuser, dbpass, user):
    with settings(host_string=host):
        try: exists = user_exists(dbaddr, dbname, dbuser, dbpass, user)
        except CheckFailed:
            return None
        if exists:
            return 1
        else:
            return 0

def get_ssh_config(dotsshconfig):
    config = SSHConfig()
    ssh_config = open(dotsshconfig, 'r')
    config.parse(ssh_config)
    ssh_config.close()
    return config

ssh_config = get_ssh_config('/home/%s/.ssh/config' % env['user'])

if __name__ == '__main__':
    import sys
    (_, host, dbaddr, dbname, dbuser, dbpass, username) = sys.argv
    h_info = ssh_config.lookup(host)
    h = '%s@%s:%s' % (h_info['user'], h_info['hostname'], h_info['port'])
    exists = check_if_user_exists(h, dbaddr, dbname, dbuser, dbpass, username)
    if exists is None:
        print 'failed to run check'
    elif exists:
        print 'found the user'
    else:
        print 'no such user'
