"""
LINKEDIN EMPLOYEE SCRAPER ROBOT

scrapes linkedin employees per company url,
including those out of your network, exploiting
'also viewed list' behaviour

!! this doesn't work with linkedin's new design,
where above leak was closed

writes Name, Title, Location, URL to `.json`

uses redis caching server to reduce number
of requests to linkedin servers

"""
import os
import traceback
from multiprocessing import Process, Manager
from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.remote.command import Command
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import NoSuchElementException
import time
import math
import json
import re
import inspect
import random
from urlparse import urlparse
import shutil
import subprocess
import redis


class RobotError(Exception):
    pass


class EmpCntException(Exception):
    pass


class NewLookException(Exception):
    pass


# monkey patch the click method:
def webelement_click(self):
    """ override click with time.sleep to fix the bug when it's not clicking"""
    # print 'clicking element'
    self._execute(Command.CLICK_ELEMENT)
    time.sleep(2)


WebElement.click = webelement_click


# monkey patch and log get TODO comment out after debug
# def webdriver_get(self, x_url):
#     """
#     Loads a web page in the current browser session.
#     """
#     print 'getting url: %s' % x_url
#     self.execute(Command.GET, {'url': x_url})
#
#
# WebDriver.get = webdriver_get


def run_redis_srv():
    """
    spawn redis server in background
    kill existing server if present
    backup rdb if present
    """
    os.system('taskkill /F /IM redis-server.exe')

    redis_dir = os.path.abspath('.\\redis')
    redis_db = redis_dir + '\\alv_cache.rdb'
    redis_conf = redis_dir + '\\redis.conf'
    redis_srv = redis_dir + '\\redis-server.exe'

    if os.path.isfile(redis_db):
        shutil.copy(redis_db,
                    redis_dir + '\\cache_bak\\avl_bak_%d' % int(time.time()))

    proc = subprocess.Popen([redis_srv, redis_conf])
    print 'redis-server started with PID %s on localhost:6739' % proc.pid


class LinkedinRobot(object):
    """ browser robot class
    credentials example: tuple ('login', 'password'),
    or list ['login', 'password']
    self.sleep - time.sleep() before action in seconds
    """

    def __init__(self, credentials, socks_port, debug=False):
        self.socks_port = socks_port
        self.debug = debug
        self._debug_kill_all()
        self._check_gecko()
        self.fp = webdriver.FirefoxProfile()
        self.fp_set_prefs()
        self.driver = webdriver.Firefox(
            firefox_profile=self.fp,
            log_path=os.devnull,
            firefox_binary='C:\\Program Files\\Mozilla Firefox\\firefox.exe'
        )
        self.driver.implicitly_wait(30)
        self.login = credentials[0]
        self.password = credentials[1]
        self.sleep = 0
        self.r = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.r.ping()
        self.dict_id = 0

    # print calls for debug
    # def __getattribute__(self, name):
    #     """ debug code for printing method calls """
    #     returned = object.__getattribute__(self, name)
    #     if inspect.isfunction(returned) or inspect.ismethod(returned):
    #         print 'called ', returned.__name__
    #     return returned

    def fp_set_prefs(self):
        prefs = {
            'extensions.logging.enabled': False,
            'datareporting.healthreport.about.reportUrl': '',
            'datareporting.healthreport.infoURL': '',
            'datareporting.healthreport.logging.consoleEnabled': False,
            'datareporting.healthreport.service.enabled': False,
            'datareporting.healthreport.service.firstRun': False,
            'datareporting.healthreport.uploadEnabled': False,
            'datareporting.policy.dataSubmissionEnabled': False,
            'app.update.auto': False,
            'app.update.enabled': False,
            'browser.search.countryCode': 'US',
            'browser.search.region': 'US',
            'browser.search.update': False,
            'extensions.update.enabled': False,
            'media.peerconnection.enabled': False,
            'browser.selfsupport.url': '',
            'network.proxy.type': 1,
            'network.proxy.socks': 'localhost',
            'network.proxy.socks_port': int(self.socks_port),
            'network.proxy.socks_remote_dns': True,

            # https://github.com/pyllyukko/user.js/blob/master/user.js
            'gfx.direct2d.disabled': True,
            'layers.acceleration.disabled': True,
            'geo.enabled': False,
            'dom.mozTCPSocket.enabled': False,
            'dom.netinfo.enabled': False,
            'dom.webaudio.enabled': False,
            'media.navigator.enabled': False,
            'dom.battery.enabled': False,
            'dom.telephony.enabled': False,
            'beacon.enabled': False,
            'dom.event.clipboardevents.enabled': False,
            'dom.enable_performance': False,
            'media.webspeech.recognition.enable': False,
            'media.getusermedia.screensharing.enabled': False,
            'device.sensors.enabled': False,
            'browser.send_pings': False,
            'browser.send_pings.require_same_host': True,
            'dom.gamepad.enabled': False,
            'dom.vr.enabled': False,
            'dom.webnotifications.enabled': False,
            'webgl.disabled': True,
            'webgl.enable-debug-renderer-info': False,
            'camera.control.face_detection.enabled': False,
            'clipboard.autocopy': False,
            'keyword.enabled': False,
            'browser.urlbar.trimURLs': False,
            'browser.fixup.alternate.enabled': False,
            'network.manage-offline-status': False,
            'security.mixed_content.block_active_content': True,
            'security.mixed_content.block_display_content': True,
            'javascript.options.methodjit.chrome': False,
            'javascript.options.methodjit.content': False,
            'network.jar.open-unsafe-types': False,
            'security.xpconnect.plugin.unrestricted': False,
            'security.fileuri.strict_origin_policy': True,
            'javascript.options.asmjs': False,
            'gfx.font_rendering.opentype_svg.enabled': False,
            'browser.urlbar.filter.javascript': True,
            'media.video_stats.enabled': False,
            'browser.display.use_document_fonts': 0,
            'extensions.getAddons.cache.enabled': False,
            'plugin.state.flash': 0,
            'plugins.click_to_play': True,
            'devtools.webide.enabled': False,
            'devtools.webide.autoinstallADBHelper': False,
            'devtools.webide.autoinstallFxdtAdapters': False,
            'devtools.debugger.remote-enabled': False,
            'devtools.chrome.enabled': False,
            'devtools.debugger.force-local': True,
            'toolkit.telemetry.enabled': False,
            'toolkit.telemetry.unified': False,
            'experiments.supported': False,
            'experiments.enabled': False,
            'browser.uitour.enabled': False,
            # 'privacy.trackingprotection.enabled': True,
            # 'privacy.trackingprotection.pbmode.enabled': True,
            'privacy.resistFingerprinting': True,
            'pdfjs.disabled': True,
            'browser.newtabpage.enhanced': False,
            'browser.newtab.preload': False,
            'browser.newtabpage.directory.ping': '',
            'browser.newtabpage.directory.source': 'data:text/plain,{}',
            'loop.logDomains': False,
            'browser.safebrowsing.downloads.remote.enabled': False,
            'browser.pocket.enabled': False,
            'extensions.pocket.enabled': False,
            'network.prefetch-next': False,
            'browser.search.geoip.url': '',
            'network.dns.disablePrefetch': True,
            'network.dns.disablePrefetchFromHTTPS': True,
            'network.dns.blockDotOnion': True,
            'network.predictor.enabled': False,
            'network.seer.enabled': False,
            'browser.search.suggest.enabled': False,
            'browser.urlbar.suggest.searches': False,
            'browser.casting.enabled': False,
            'media.gmp-gmpopenh264.enabled': False,
            'media.gmp-manager.url': '',
            'network.http.speculative-parallel-limit': 0,
            'browser.aboutHomeSnippets.updateUrl': '',
            'security.sri.enable': True,

            # disable images
            'permissions.default.image': 2,

        }

        for k, val in prefs.items():
            self.fp.set_preference(k, val)

    @staticmethod
    def _check_gecko():
        """ for firefox driver to work gecko must be in path """
        gecko_path = 'c:\\Program Files\\Geckodriver\\'
        if 'Geckodriver' not in os.environ['PATH']:
            os.environ['PATH'] += os.pathsep + gecko_path

    def _debug_kill_all(self):
        if self.debug:
            # print 'debug mode is on'
            # the /F switch forces the kill, otherwise it sends term signal
            os.system('taskkill /F /IM geckodriver.exe')
            os.system('taskkill /F /IM plugin-container.exe')
            os.system('taskkill /F /IM firefox.exe')

    def logout(self):
        selector = '#account-sub-nav > div > div.account-sub-nav-body > ' \
                   'ul > li.self > div > span > span.act-set-action > a'
        logout_link = self.driver.find_element_by_css_selector(selector)
        time.sleep(self.sleep)
        self.driver.get(logout_link.get_attribute('href'))

    def quit(self):
        self.logout()
        time.sleep(self.sleep)
        self.driver.quit()
        # plugin-container will crash and win will throw err from werfault.exe
        # no better fix at the moment
        os.system('taskkill /F /IM werfault.exe')

    def log_in(self):

        self.get_if_not_current('https://www.linkedin.com')

        # quick and dirty test for new style, if new - raise exception
        test = 'Be great at what you do'
        src = self.driver.page_source
        if test in src:
            login_email = self.driver.find_element_by_id('login-email')
            login_password = self.driver.find_element_by_id('login-password')
            login_submit = self.driver.find_element_by_id('login-submit')

            login_email.send_keys(self.login)
            login_password.send_keys(self.password)
            login_submit.click()
        else:
            raise NewLookException('new look, cannot continue')

    @staticmethod
    def company_url_from_id(company_id):
        company_url = 'https://www.linkedin.com/company/%s' % company_id
        return company_url

    def get_employee_count(self, company_url):
        self.get_if_not_current(company_url)
        el = self.driver.find_element_by_css_selector(
            '#biz-connectedness-top > div > div > ul.stats > li > a')
        return el.text

    @staticmethod
    def get_id_from_company_url(company_url):
        return company_url.split('/')[-1]

    def get_employee_urls_per_company(self, company_url):
        """
        0. start with company url, go to 'see all'
        1. get employee count and page count
        2. generate page links
        3. go to each page link, scrape employee list (1-10 per link)
        4. return all found emloyee urls for company
        """

        emplist_id = 'emplist:' + company_url
        # first check in cache
        if self.r.exists(emplist_id):
            all_employee_urls = json.loads(self.r.get(emplist_id))
            return all_employee_urls

        company_id = company_url.split('/')[-1]
        see_all_url = 'https://www.linkedin.com/vsearch/p?f_CC=%s' % company_id
        self.get_if_not_current(see_all_url)

        employees_count = self.driver.find_element_by_css_selector(
            '#results_count > div > p > strong').text

        if ',' in employees_count:
            # handle empl_count with comma, ie 1,087
            employees_count = employees_count.replace(',', '')

        page_count = int(math.ceil(float(employees_count) / 10))

        # temprorary hack to avoid companies with too much employees
        # TODO refactor into a setting later
        if int(employees_count) > 1500:
            raise EmpCntException(
                'too much employees: %s' % employees_count)
        elif int(employees_count) <= 1:
            raise EmpCntException(
                'too little employees: %s' % employees_count)

        all_employee_urls = []
        for page_num in range(1, page_count + 1):
            base_url = 'https://www.linkedin.com/vsearch/p?f_CC='
            url = base_url + '%s&page_num=%d&pt=people' % (company_id, page_num)
            employee_url_list_per_page = self.get_employee_urls_per_page(url)
            for employee_url in employee_url_list_per_page:
                all_employee_urls.append(employee_url)

        # make sure total employee links scraped equal total info from linkedin
        empl_found = len(all_employee_urls)
        empl_total = int(employees_count)
        assert empl_found == empl_total, \
            'bad employee count: found: %s total: %s' % (empl_found, empl_total)

        # save to cache
        self.r.set(emplist_id, json.dumps(all_employee_urls))
        self.r.save()

        return all_employee_urls

    def get_employee_urls_per_page(self, url):
        """
        get list of employees on page
        :return list of tuples (empl_url_clean, display_name, inmail_url)
        """
        self.get_if_not_current(url)
        employee_url_list = []
        employee_list = self.driver.find_elements_by_css_selector(
            '#results > li')
        for employee in employee_list:
            # 'mod result' to check that li is not advertisment from linkedin
            if self.f_startswith(employee.get_attribute('class'), 'mod result'):
                employee_url_raw = employee.find_element_by_class_name(
                    'main-headline').get_attribute('href')
                employee_url_clean = self.parse_profile(employee_url_raw,
                                                        token=True)
                inmail_url = self.inmail_url_from_search_url(employee_url_raw)
                employee_url_list.append((employee_url_clean, inmail_url))
        return employee_url_list

    def get_employee_full_name(self, employee_page_url, geturl=True):
        if geturl:
            self.get_if_not_current(employee_page_url)
        el = self.driver.find_element_by_css_selector(
            '#name > h1 > span > span')
        return el.text

    @staticmethod
    def inmail_url_from_search_url(raw_url):
        """
        generate inmail url based on url from search
        """
        ptrn = re.compile(r'targetId%3A([0-9]+)%')
        found_id = re.search(ptrn, raw_url)
        assert found_id is not None
        base = 'https://www.linkedin.com/requestList?displayProposal=&destID='
        inmail_url = base + '%s' % found_id.group(1)
        return inmail_url

    def get_employee_name(self, inmail_url):
        """
        get employee name using send inmail function leak
        if send inmail option is hidden, construct inmail url manually
        handle two types of landing pages
        """
        self.get_if_not_current(inmail_url)
        time.sleep(self.sleep)
        if 'reach out' in self.driver.page_source:  # type 1
            el = self.driver.find_element_by_css_selector('.headline')
            name = el.text.split(' ')[4]
        else:  # type 2
            name = self.driver.find_element_by_css_selector(
                '#aq-header > div > div > h1').text.split(' ')[-1]
        assert name is not None
        return name

    def get_employee_title(self, employee_page_url, geturl=True):
        """ :return unicode object """
        if geturl:
            self.get_if_not_current(employee_page_url)
        try:
            title = self.driver.find_element_by_css_selector(
                '#headline > p.title')
        except NoSuchElementException, e:
            print 'worker %s failed to get title, url: %s' % (
                self.login, self.driver.current_url)
            print e
            return ''

        return title.text

    def get_employee_geo(self, employee_page_url, geturl=True):
        """ :return tuple of unicode (geo, insdustry) """
        if geturl:
            self.get_if_not_current(employee_page_url)

        loc = self.driver.find_element_by_css_selector(
            '#location > dl')
        elems_in_loc = loc.find_elements_by_css_selector('*')
        geo = self.driver.find_element_by_css_selector(
            '#location > dl > dd > span.locality > a').text
        industry = 'Not specified'

        # locality is always present, industry may not be present
        # if industry is not present, elements count in loc == 4,
        # if present == 7
        if len(elems_in_loc) >= 5:
            industry = self.driver.find_element_by_css_selector(
                '#location > dl > dd.industry > a').text

        return geo, industry

    def str_in_source(self, test_str):
        if test_str in self.driver.page_source:
            return True
        else:
            return False

    def search_cache_alv_list(self, search_id):
        """ :return list if found, else empty list """
        if self.r.exists(search_id):
            data = json.loads(self.r.get(search_id))
            alv_list = data['alv_list']
            return alv_list
        else:
            return []

    def search_id_from_url(self, url):
        """ url should be of type /profile/ """
        if self.f_startswith(url, 'https://www.linkedin.com/profile'):
            search_id = self.parse_profile(url)
            return search_id
        else:
            raise RobotError('bad search id from url: %s' % url)

    def get_alv_list(self):
        """
        need to start from empl page url
        type () -> tuple (name, title, profile_url)
        """
        try:
            people_also_viewed = self.driver.find_elements_by_css_selector(
                'ul.browse-map-list > li')
        except NoSuchElementException, e:
            print 'worker %s failed to find alv_list. url: %s' % (
                self.login, self.driver.current_url)
            print e
            return []

        people_also_viewed_list = []
        for child in people_also_viewed:
            elem = child.find_element_by_css_selector('h4 > a')
            full_url = elem.get_attribute('href')

            profile_url = self.parse_profile(full_url, token=True)
            name = elem.text
            title = child.find_element_by_class_name('browse-map-title').text

            people_also_viewed_list.append((name, title, profile_url))

        return people_also_viewed_list

    def get_people_also_viewed(self, employee_page_url, search_id='',
                               geturl=True, child_list=False):
        """
        presearch also_viewed_list in cache
        go to employee page, grab 'people also viewed' list
        return list of tuples with format (name, title, geo, profile_url)
        """
        if not search_id:
            search_id = self.search_id_from_url(employee_page_url)

        assert len(search_id) >= 1
        cache_alv_list = self.search_cache_alv_list(search_id)

        if not cache_alv_list:
            if geturl:
                self.get_if_not_current(employee_page_url)

            alv_list = self.get_alv_list()
            crnt_url = self.driver.current_url

            if child_list:
                save_name = self.get_employee_full_name(crnt_url, geturl=False)
                data = {
                    'id_type': 'child',
                    'name': save_name,
                    'title': self.get_employee_title(crnt_url, geturl=False),
                    'geo': self.get_employee_geo(crnt_url, geturl=False),
                    'const_url': self.parse_base(crnt_url),
                    'alv_list': alv_list,
                }
                self.r.set(search_id, json.dumps(data))
                self.r.save()

            return alv_list

        return cache_alv_list

    def get_and_sleep(self, url):
        self.driver.get(url)
        time.sleep(self.sleep)

    @staticmethod
    def f_startswith(string_to_compare, string_startswith):
        """
        40-50% faster then startswith
        unsafe - can lead to bugs if type is not str or unicode
        type checks cause slowdown
        """
        return string_to_compare[:len(string_startswith)] == string_startswith

    def both_url_startwith_str(self, url, crnt_url, start_str):
        """ :return True/False """
        url_bool = self.f_startswith(url, start_str)
        crnt_url_bool = self.f_startswith(crnt_url, start_str)
        return url_bool and crnt_url_bool

    @staticmethod
    def parse_both_urls(url, crnt_url, parse_method, token=False):
        """ :return (url, crnt_url) """
        if token:
            return parse_method(url, token=token), parse_method(crnt_url,
                                                                token=token)
        else:
            return parse_method(url), parse_method(crnt_url)

    def get_if_not_current(self, url):
        """
        !! this is Work in Progress

        if base current page url != base page url to get --> get page, sleep

        get_if_not_current allows to use most methods as standalone
        and shave off redundant gets when methods are used in sequence

        comparison is lazy. see parse_base(), parse_profile(), parse_vsearch()

        !!! can cause a redirect loop bug
        (no way to check status code in selenium, also linkedin asshole code)
        so far this behaviour has been noticed on pages
        with profile/view?id which lead to /in/username-123

        so far the links used in project are of type: /in/, /vsearch/, /profile/
        only /vsearch/ and /profile/ have special parsing methods,
        other links use parse_base() which can cause bugs.
        need to add new parser and compare code methods if parse_base()
        is not sufficient when visiting links of other type
        """
        # TODO: maybe implement redirect loop fallback
        # add urls to global history
        # crnt_url is 0, url is 1. if history pattern is
        # [1,0,1,0,1,0,1,0,1,0,1,0,1,0] repeats for X times in a row,
        # raise exception

        url = url.encode('utf-8')
        crnt_url = self.driver.current_url.encode('utf-8')

        # if base not equal, get crnt_url and return
        if self.parse_base(url) != self.parse_base(crnt_url):
            self.get_and_sleep(url)
            return

        # need to parse each type of urls differently since
        # they have different parameters
        elif self.both_url_startwith_str(url, crnt_url,
                                         'https://www.linkedin.com/profile'):
            url, crnt_url = self.parse_both_urls(url, crnt_url,
                                                 self.parse_profile, token=True)
        elif self.both_url_startwith_str(url, crnt_url,
                                         'https://www.linkedin.com/vsearch'):
            url, crnt_url = self.parse_both_urls(url, crnt_url,
                                                 self.parse_vsearch)
        else:
            # this can lead to bugs
            url, crnt_url = self.parse_both_urls(url, crnt_url, self.parse_base)

        if not url == crnt_url:
            self.get_and_sleep(url)
            return

    def get_employee_basic_info(self, empl_url, inmail_url, search_id,
                                geturl=False, fullname=False):
        """ :return tupl of (unicode, unicode, unicode, list)"""
        if geturl:
            assert len(empl_url) >= 1
            self.get_if_not_current(empl_url)

        time.sleep(2)  # todo see if this fixes random 'NoSuchElementException'
        title = self.get_employee_title(empl_url, geturl=geturl)
        geo = self.get_employee_geo(empl_url, geturl=geturl)
        also_viewed_list = self.get_people_also_viewed(empl_url, geturl=geturl,
                                                       search_id=search_id)
        if not fullname:
            assert len(inmail_url) >= 1
            name = self.get_employee_name(inmail_url)
        else:
            name = self.get_employee_full_name(empl_url, geturl=geturl)

        return name, title, geo, also_viewed_list

    @staticmethod
    def parse_base(url):
        """ :return base url """
        u = urlparse(url)
        return u.scheme + '://' + u.netloc + u.path

    @staticmethod
    def parse_profile(url, token=False):
        """
        :return base profile_id url without token
        by default and with token if token=True
        """
        u = urlparse(url)
        q = urlparse(url).query.split('&')
        base = u.scheme + '://' + u.netloc + u.path + '?' + q[0]
        if token:
            return base + '&' + q[1] + '&' + q[2]
        else:
            return base

    @staticmethod
    def parse_vsearch(url):
        """ :return example: https://www.linkedin.com/vsearch/p?f_CC=3905248 """
        u = urlparse(url)
        q = urlparse(url).query
        return u.scheme + '://' + u.netloc + u.path + '?' + q

    def child_alv_list_exists(self, child_id):
        """
        :return True, [child_alv_list]
        or
        :return False, []
        """
        if self.r.exists(child_id):
            # if child url key exists, get master key from it
            data = json.loads(self.r.get(child_id))
            return data['alv_list']

        return []

    def check_find_match_done(self, search_id):
        if self.r.exists(search_id):
            data = json.loads(self.r.get(search_id))
            return data['name'], data['title'], data['geo'], data['const_url']
        return []

    def find_match_in_inurl(self, crnt_url, inmail_url, search_id):
        """
        if current url of type /in/, can access person directly
        without matching
        :return name, title, geo, clean_url
        clean url to unhide all info, go to unhidden page, scrape info
        """
        clean_url = self.parse_base(crnt_url)
        self.get_and_sleep(clean_url)

        # TODO this is dirty fix, refactor
        # weird behaviour example: https://www.linkedin.com/in/sabrinagra%C3%B1a
        # page returns Profile Not Found, should return profile page
        if 'Profile Not Found' is self.driver.page_source:
            print 'worker: %s encountered weird bug ' \
                  'Profile Not Found, url: %s' % (
                      self.login, self.driver.current_url)
            return '', '', '', []

        if self.r.exists(search_id):
            data = json.loads(self.r.get(search_id))
            return data['name'], data['title'], data['geo'], data['const_url']

        name, title, geo, alv_list = self.get_employee_basic_info(crnt_url,
                                                                  inmail_url,
                                                                  search_id,
                                                                  fullname=True)

        data = {
            'id_type': 'search',
            'name': name,
            'title': title,
            'geo': geo,
            'const_url': clean_url,
            'alv_list': alv_list,
        }
        self.r.set(search_id, json.dumps(data))
        self.r.save()

        return name, title, geo, clean_url

    def get_child_alv_list(self, url, child_id):
        """
        if child_id have been visited, the alv_list data from there was saved
        """
        child_alv_list_in_cache = self.child_alv_list_exists(child_id)
        if child_alv_list_in_cache:
            return child_alv_list_in_cache
        else:
            child_alv_list = self.get_people_also_viewed(url, child_list=True)
            return child_alv_list

    @staticmethod
    def name_in_list(name, child_alv_list):
        """
        return list of indexes in alv_list
        where names match, or empty list
        """
        name_in_list = [i for i, x in enumerate(child_alv_list) if
                        x[0].split(' ')[0] == name]
        if len(name_in_list) > 0:
            return name_in_list
        else:
            return []

    @staticmethod
    def filter_child_list(list_to_filter, child_alv_list):
        """
        filter out items with non-matching names, then try match title to name
        'x' is indexes from name_in_list()
        """
        return [child_alv_list[x] for x in list_to_filter]

    def filtered_alv_list(self, name, child_alv_list):
        """
        check if any 'names' in alv_list match 'name'
        return list of matches or empty
        """
        name_in_list = self.name_in_list(name, child_alv_list)
        if name_in_list:
            filtered_child_list = self.filter_child_list(name_in_list,
                                                         child_alv_list)
            return filtered_child_list
        else:
            return []

    def find_match_in_also_viewed(self, employee_page_url, inmail_url):
        """
        get name, title, geo, [master_also_viewed_list] for master
        match 3/3: name, title, geo

        search 10 children of master's [also_viewed_list]
        for match in each of 10 children of [child_also_viewed_list]
        (max total 10x10 searches)

        return tuple(n,t,g,u) of matches when 1st 3/3 match is found

        return tuple of (FAIL,FAIL,FAIL, url) if no 3/3 match is found

        return ('PAGE', 'NOT', 'FOUND', url) if weird behaviour from
        find_match_in_inurl()

        """
        search_id = self.search_id_from_url(employee_page_url)

        fm_done = self.check_find_match_done(search_id)
        if fm_done:
            n, t, g, u = fm_done[0], fm_done[1], fm_done[2], fm_done[3]
            return n, t, g, u

        self.get_and_sleep(employee_page_url)
        # if current url of type /in/,
        # can access person directly without matching
        crnt_url = self.driver.current_url

        if self.f_startswith(crnt_url, 'https://www.linkedin.com/in/'):
            n, t, g, u = self.find_match_in_inurl(crnt_url, inmail_url,
                                                  search_id)
            if not (n or t or g or u):
                return 'PAGE', 'NOT', 'FOUND', crnt_url

            return n, t, g, u

        mr_name, mr_title, mr_geo, mr_alv_list = \
            self.get_employee_basic_info(employee_page_url, inmail_url,
                                         search_id)

        for ch_name, ch_title, ch_url in mr_alv_list:

            child_id = self.search_id_from_url(ch_url)
            ch_alv_list = self.get_child_alv_list(ch_url, child_id)
            fch_alv_list = self.filtered_alv_list(mr_name, ch_alv_list)

            for fch_name, fch_title, fch_url in fch_alv_list:
                fch_geo = self.get_employee_geo(fch_url)
                if self.match_title_and_geo(mr_title, mr_geo, fch_title,
                                            fch_geo):
                    matched_url = self.parse_base(self.driver.current_url)
                    data = {
                        'id_type': 'search',
                        'name': fch_name,
                        'title': fch_title,
                        'geo': fch_geo,
                        'const_url': matched_url,
                        'alv_list': mr_alv_list,
                    }
                    self.r.set(search_id, json.dumps(data))
                    self.r.save()

                    return fch_name, fch_title, fch_geo, matched_url

        return 'FAIL', 'FAIL', 'FAIL', employee_page_url

    @staticmethod
    def match_title_and_geo(mr_title, mr_geo, ch_title, ch_geo):
        """ match master title and geo to child title and geo """
        if mr_title == ch_title:
            if ch_geo == mr_geo:
                return True
        return False

    def scrape_all_per_company_url(self, company_url):
        """
        parse and scrape all employees of company
        :param company_url:
        :return dict{url, geo, name, title}
        """

        found_employee_dict = {}

        company_emloyee_urls = self.get_employee_urls_per_company(company_url)

        urls_found = len(company_emloyee_urls)
        self.dict_id = 0

        for data_tuple in company_emloyee_urls:
            url = data_tuple[0]
            inmail_url = data_tuple[1]
            found_employee_dict[
                str(self.dict_id)] = self.find_match_in_also_viewed(url,
                                                                    inmail_url)
            self.dict_id += 1

        assert urls_found == self.dict_id

        return found_employee_dict


def remove_from_complist_file(comp_id):
    """ remove company from scrape list if scrape was successful """
    filtered = []
    with open('companies_list_filtered.txt', 'r') as cinfile:
        for line in cinfile:
            line = line.strip()
            if line:
                if line == comp_id:
                    pass
                else:
                    filtered.append(line)

    with open('companies_list_filtered.txt', 'w') as coutfile:
        for line in filtered:
            coutfile.write(line + '\n')


def worker(comp_list, creds):
    """
    multiprocessing worker

    HAS DATA:
        creds: email:login:port
        list of companies to scrape
        err_log file
        success_log file
        empcount_log for errors of employee count
    """
    creds = creds.split(':')

    w_name = creds[0]
    w_pass = creds[1]
    w_port = creds[2]

    print w_name  # todo comment out after debug

    err_log = './logs/' + w_name + '_err.log'
    ok_log = './logs/' + w_name + '_ok.log'
    empcount_log = './logs/empcount_err.log'
    err_count = 0

    my_robot = LinkedinRobot(credentials=[w_name, w_pass], socks_port=w_port)
    my_robot.sleep = 5
    my_robot.log_in()

    for iteration in range(len(comp_list)):
        if err_count >= 1000:
            print '--------------------------------------'
            print 'worker: %s MAX ERR COUNT (%s) REACHED. breaking.' % (
                err_count, w_name)
            print '--------------------------------------'
            break
        try:
            comp_id = comp_list.pop()
        except IndexError, e:
            msg = 'worker: %s Companies list is empty, breaking' % w_name
            with open(err_log, 'a') as outfile:
                outfile.write('%s,%s\n' % (msg, e))
            break
        except IOError, e:
            print 'worker: %s failed to pop from list. err: %s' % (w_name, e)
            time.sleep(5)
            comp_id = comp_list.pop()

        sleep_dur = random.randint(30, 60)

        try:
            print 'worker: %s going to scrape company %s' % (w_name, comp_id)
            target_company = 'https://www.linkedin.com/company/%s' % comp_id
            data_to_write = my_robot.scrape_all_per_company_url(target_company)

            my_robot.r.save()

            with open('./data/employee_list_%s.json' % comp_id, 'w') as outfile:
                my_data = json.dumps(data_to_write)
                my_data = json.loads(my_data)
                outfile.write(json.dumps(my_data, indent=4))

            with open(ok_log, 'a') as outfile:
                outfile.write('%s OK\n' % comp_id)

            remove_from_complist_file(comp_id)

            print '%s finished scraping %s with SUCCESS' % (w_name, comp_id)
            print '%s going to sleep for %s sec' % (w_name, sleep_dur)

            time.sleep(sleep_dur)

        except EmpCntException, e:
            # not critical err
            remove_from_complist_file(comp_id)
            with open(empcount_log, 'a') as outfile:
                outfile.write('%s,%s\n' % (comp_id, e))

        except NewLookException, e:
            # not critical err
            with open(err_log, 'a') as outfile:
                outfile.write('%s,%s\n' % (comp_id, e))
                # relaunch robot
            time.sleep(30)
            my_robot.quit()
            my_robot = LinkedinRobot(credentials=creds, socks_port=w_port)
            my_robot.sleep = 5
            my_robot.log_in()

        except NoSuchElementException, e:
            # catch unexpected NoSuchElement exceptions
            # known errors:
            err_ban = 'LinkedIn is Momentarily Unavailable'  # bot is banned
            err_conn = 'The proxy server is refusing connections'  # proxy fail
            err_look = 'artdeco'  # account was reset to new style

            trace = traceback.format_exc()

            if err_ban in my_robot.driver.page_source:
                print 'worker: %s is banned, going to sleep for 5 hours'
                comp_list.insert(0, comp_id)
                time.sleep(60 * 60 * 5)
                continue
            elif err_conn in my_robot.driver.page_source:
                print 'worker: %s failed to connect, quitting' % w_name
                my_robot.quit()
                return
            elif err_look in my_robot.driver.page_source:
                print 'worker: %s was reset to new look, quitting' % w_name
                my_robot.quit()
                return
            else:
                print 'worker: %s unexpected NoSuchElement exception' % w_name
                print trace
                err_count += 1

            with open(err_log, 'a') as outfile:
                url = my_robot.driver.current_url
                outfile.write(
                    'comp_id: %s, error: %s\n url: %s\n' % (comp_id, e, url))
                outfile.write(trace)

        except Exception, e:
            # all these exceptions are potentially critical
            # print to console and write to log for later debug
            err_count += 1

            print '---------------------------------------------------'
            print '%s encountered critical error: %s' % (w_name, e)
            print '%s failed to scrape company %s, skipping' % (w_name, comp_id)
            print 'debug url: %s ' % my_robot.driver.current_url
            print '---------------------------------------------------'

            with open(err_log, 'a') as outfile:
                trace = traceback.format_exc()
                outfile.write('%s,%s\n' % (comp_id, e))
                outfile.write(trace)

            # push company back to comp_list[0], can lead to bugs if neither bot
            # can finish scraping, then after 1000 fails each will stop
            comp_list.insert(0, comp_id)

            print '%s going to sleep for %s sec after critical error' % (
                w_name, sleep_dur)
            time.sleep(sleep_dur)


def run():
    # todo clean cache where title == '--' change to ''

    # make sure no old procs left running
    os.system('taskkill /F /IM plugin-container.exe')
    os.system('taskkill /F /IM firefox.exe')
    os.system('taskkill /F /IM geckodriver.exe')
    os.system('taskkill /F /IM helper.exe')

    bot_creds_list = []  # bot credentials, format: 'login:password:port'
    with open('creds_list.txt', 'r') as infile:
        for line in infile:
            line = line.strip()
            if line:
                bot_creds_list.append(line)

    # launch redis cache server
    run_redis_srv()

    # populate companies list from file
    companies_list = Manager().list()
    with open('companies_list_filtered.txt') as infile:
        for cid in infile:
            cid = cid.strip()
            if cid:
                companies_list.append(cid)

    proc_list = []

    # set to limit max bots from creds_list[]
    limit = len(bot_creds_list)

    for bot_creds in bot_creds_list[0:limit]:
        worker_proc = Process(target=worker, args=(companies_list, bot_creds,))
        proc_list.append(worker_proc)

    for worker_num, worker_proc in enumerate(proc_list):
        worker_proc.start()
        print 'started worker: %s' % worker_num
        time.sleep(random.randint(55, 65))

    for worker_num, worker_proc in enumerate(proc_list):
        worker_proc.join()
        print 'joined worker: %s' % worker_num


if __name__ == '__main__':
    run()
