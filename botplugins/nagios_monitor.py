from botplugin import BotPlugin
import httplib2
import secret
import threading
from BeautifulSoup import BeautifulSoup
from datetime import datetime

alert_icon = {'CRITICAL':':finnadie:',
              'WARNING' :':rage3:',
              'OK'      :':smiley:'}

class NagiosMonitor(BotPlugin):
    fetch_interval = 60
    def setup(self, bot, options):
        self.bot = bot
        self.last_run = datetime.now()
        #self.last_run = to_datetime("2012-07-19 10:22:42")
        self.notify_channel = 'testchan'

        self.timer = threading.Timer(self.fetch_interval, self.notify_channel_of_events)
        self.timer.start()

    def teardown(self):
        if self.timer:
            if self.timer.isAlive():
                self.timer.cancel()
            self.timer = None

    def notify_channel_of_events(self):
        html = open_page(secret.NAGIOS_URL)
        if not html:
            return False

        events = get_latest_events(html)
        for event in events:
            # Check if top event is more than the 
            # last saved run time
            if event['time'] <= self.last_run:
                continue
            msg = "{0} {1} | {2} | {3} | {4}".format(alert_icon[event['level']],
                                                      event['time'],
                                                      event['service'],
                                                      event['host'],
                                                      event['info'])
            self.bot.say_public(self.notify_channel, msg)

        # It is so we have a new last_run time
        self.last_run = events[-1]['time']
        print "New last run time = ", self.last_run

        # start a new timer if existing timer wasn't cancelled,
        if self.timer and self.fetch_interval:
            self.timer = threading.Timer(self.fetch_interval, 
                                         self.notify_channel_of_events)
            self.timer.start()
        else:
            logging.warning('Not setting a new monitor timer.')


def open_page(nagios_url):
    #h = httplib2.Http(".cache")
    #h.add_credentials(secret.NAGIOS_USER, secret.NAGIOS_PASS)
    #resp, content = h.request(nagios_url)
    ## Only return content if we actually found something
    #if resp['status'] == '200':
    #    return content
    #return None
    return open("/tmp/index.html").read()

def get_latest_events(html):
    """
    Scrapes the Nagios Notifications page
    for alerts
    """
    last_check = get_last_check()
    output = []
    soup = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES)
    trs = soup.find('table', 'notifications').findAll('tr')
    for tr in trs[2:]:
        td = tr.findAll('td')
        host = td[0].a.text
        try:
            service = td[1].a.text
        except AttributeError:
            service = "N/A"
        level = td[2].text
        time = td[3].text
        info = td[6].text
        output.append({'host'   : host,
                       'service': service,
                       'level'  : level,
                       'time'   : to_datetime(time),
                       'info'   : info})
    return output

def get_last_check():
    return datetime.now()
    #return to_datetime("2012-07-18 15:22:06")

def to_datetime(time_string):
    format_string = "%Y-%m-%d %H:%M:%S"
    return datetime.strptime(time_string, format_string)

if __name__ == "__main__":
    last_run = to_datetime("2012-07-19 10:22:42")
    html = open_page(secret.NAGIOS_URL)
    if html:
        events = get_latest_events(html)
        for event in events:
            if event['time'] <= last_run:
                continue
            print "{0} {1} | {2} | {3} | ({4}) {5}".format(alert_icon[event['level']],
                                                           event['time'],
                                                           event['service'],
                                                           event['host'],
                                                           event['level'],
                                                           event['info'])

        # It is so we have a new last_run time
        last_run = events[0]['time']

