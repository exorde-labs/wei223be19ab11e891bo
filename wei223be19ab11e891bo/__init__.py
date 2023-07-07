"""
In this script we are going to collect data from Weibo. The idea is to navigate to this link:

https://s.weibo.com/realtime?q=[query]&rd=realtime&tw=realtime&Refer=weibo_realtime

To request the latest content linked with a specific query. Note that the query must be written in Chinese as most of
the content on this platform is only written in Chinese. Sending a query in another language will result in far less or
no content at all.

This query will return the latest posts related to the aforementioned query so can be used to gather the real time data
on the platform regarding any subject. Note that comments will not be collected through this method, as the posts will
be new and will not yet have any comments associated to them.

Every post element returned through this query is categorized under the form of cards:

<div class="card"></div>

So we can loop over those to collect the latest posts. The strategy follows this pattern:

<div class="card">
    <div class="info"></div> : username
    <div class="from">
        <a href=[link]/> : publish time of the post (time since the post was released)
    </div>
    <p class="txt"/> : content of the post
</div>

"""
import os
import random
import time
from selenium.webdriver.common.keys import Keys
from time import sleep
from datetime import datetime as datett
from datetime import timedelta, timezone
import dotenv
from pathlib import Path
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from typing import AsyncGenerator
import hashlib
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
)
import logging

# GLOBAL VARIABLES
DRIVER = None
CURRENT_DIR = Path(__file__).parent.absolute()
USER_AGENTS = [
    'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
]

global MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS # max number of consecutive old comments to make us break the loop
global CONSECUTIVE_OLD_COMMENTS_COUNT # current count of the consecutive old comments we encounter(ed)
global MAXIMUM_ITEMS_TO_COLLECT # maximum number of items to collect
global YIELDED_ITEMS # how many items we collected so far in the session

CONSECUTIVE_OLD_COMMENTS_COUNT = 0
SCROLL_SPEED_ACCELERATION = 1.15  # the scroll speed multiplier, very sensitive, modify with care

# Wait timers to conduct various actions, modify with care
SHORT_WAIT_TIME_MIN = 0.1
SHORT_WAIT_TIME_MAX = 0.3
MEDIUM_WAIT_TIME_MIN = 0.2
MEDIUM_WAIT_TIME_MAX = 0.5
LONG_WAIT_TIME_MIN = 0.5
LONG_WAIT_TIME_MAX = 2

# Typing params
TYPE_SLOW_MIN = 0.05
TYPE_SLOW_MAX = 0.3

# Posts aged over this amount of minutes will be skipped
MAX_POST_AGE_IN_MINUTES = 30 # handled below by parameters, 30min is the default.

SECONDS_AGO = "秒前"  # how many seconds since the post was put online
MINUTES_AGO = "分钟前"  # how many minutes since the post was put online

#############################################################################
#############################################################################
#############################################################################
#############################################################################
#############################################################################


def get_proxy(env):
    """
    Has not been tested
    """
    dotenv.load_dotenv(env, verbose=True)
    return load_env_variable("HTTP_PROXY", none_allowed=True)


def load_env_variable(key, default_value=None, none_allowed=False):
    """
    Has not been tested
    """
    v = os.getenv(key, default=default_value)
    if v is None and not none_allowed:
        raise RuntimeError(f"{key} returned {v} but this is not allowed!")
    return v


def get_chrome_path():
    if os.path.isfile('/usr/bin/chromium-browser'):
        return '/usr/bin/chromium-browser'
    elif os.path.isfile('/usr/bin/chromium'):
        return '/usr/bin/chromium'
    elif os.path.isfile('/usr/bin/chrome'):
        return '/usr/bin/chrome'
    elif os.path.isfile('/usr/bin/google-chrome'):
        return '/usr/bin/google-chrome'
    else:
        return None

def init_driver(headless=True, proxy=None, show_images=False, option=None, env=".weibo_env"):
    """ initiate a chromedriver instance
        --option : other option to add (str)
    """
    global DRIVER
    http_proxy = get_proxy(env)

    binary_path = get_chrome_path()
    logging.info(f"[Sina Weibo Init Driver] Selected Chrome executable path = {binary_path}")

    options = ChromeOptions()
    options.binary_location = binary_path
    logging.info("[Sina Weibo Init Driver]\tAdd options to Chrome Driver")
    options.add_argument("--disable-blink-features")  # Disable features that might betray automation
    options.add_argument(
        "--disable-blink-features=AutomationControlled")  # Disables a Chrome flag that shows an 'automation' toolbar
    options.add_experimental_option("excludeSwitches", ["enable-automation"])  # Disable automation flags
    options.add_experimental_option('useAutomationExtension', False)  # Disable automation extensions
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("disable-infobars")
    options.add_argument(f'user-agent={random.choice(USER_AGENTS)}')

    # add proxy if available
    if http_proxy is not None:
        logging.info("[Sina Weibo Init Driver]\tAdding a HTTP Proxy server to ChromeDriver: %s", http_proxy)
        options.add_argument('--proxy-server=%s' % http_proxy)
    if headless is True:
        logging.info("[Sina Weibo Init Driver]\tScraping on headless mode.")
        options.add_argument('--disable-gpu')
        options.add_argument('--headless')  # Ensure GUI is off. Essential for Docker.
    options.add_argument('log-level=3')
    if proxy is not None:
        options.add_argument('--proxy-server=%s' % proxy)
        logging.info("[Sina Weibo Init Driver]\tusing proxy :  %s", proxy)
    if not show_images:
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
    if option is not None:
        options.add_argument(option)

    driver_path = '/usr/local/bin/chromedriver'
    logging.info(f"Opening driver from path = {driver_path}")
    service = Service(driver_path)
    DRIVER = webdriver.Chrome(options=options, service=service)
    logging.info("[Sina Weibo Init Driver] Chrome driver initialized =  %s", DRIVER)

    DRIVER.set_page_load_timeout(123)
    return DRIVER


def type_slow(string, element):
    for character in str(string):
        element.send_keys(character)
        sleep(random.uniform(TYPE_SLOW_MIN, TYPE_SLOW_MAX))


def wait_random():
    sleep(random.uniform(MEDIUM_WAIT_TIME_MIN, MEDIUM_WAIT_TIME_MAX))


def wait_random_long():
    sleep(random.uniform(LONG_WAIT_TIME_MIN, LONG_WAIT_TIME_MAX))


def wait_random_short():
    sleep(random.uniform(SHORT_WAIT_TIME_MIN, SHORT_WAIT_TIME_MAX))


def smooth_scrolling():
    global DRIVER
    desired_scroll_height = int(DRIVER.execute_script("return document.body.scrollHeight"))

    for i in range(int(random.uniform(25, desired_scroll_height * 0.10)), desired_scroll_height,
                   int(desired_scroll_height * 0.20 * SCROLL_SPEED_ACCELERATION)):
        i += int(random.uniform(-desired_scroll_height * 0.05,
                                desired_scroll_height * 0.05 * SCROLL_SPEED_ACCELERATION))  # change step size every iteration
        DRIVER.execute_script("window.scrollTo(0, {});".format(i))
        wait_random()

    if int(DRIVER.execute_script("return document.body.scrollHeight")) >= desired_scroll_height:
        return


def find_element_with_timeout(_element, _timeout, _element_query):
    """
    Find a single element of the same type within the page with a timeout (in case they cannot be located/loaded)
    :param _element: the parent element we will be looking into to find what we want
    :param _timeout: the timeout counter (in seconds)
    :param _element_query: the element type we are looking for
    :return: the element we have requested, if it could be found, none otherwise
    """
    while _timeout >= 0:
        try:
            return _element.find_element(By.XPATH, _element_query)
        except:
            _timeout -= 1
            time.sleep(1)
            pass
    return None


def find_elements_with_timeout(_element, _timeout, _elements_query):
    """
    Find multiple elements of the same type within the page with a timeout (in case they cannot be located/loaded)
    :param _element: the parent element we will be looking into to find what we want
    :param _timeout: the timeout counter (in seconds)
    :param _elements_query: the element type we are looking for
    :return: the elements we have requested, if they could be found, none otherwise
    """
    while _timeout >= 0:
        try:
            return _element.find_elements(By.XPATH, _elements_query)
        except:
            _timeout -= 1
            time.sleep(1)
            pass
    return None


def start_search(_url, _query):
    """
    Start the inital search on a query. As there is a user path to follow to be able to access the latest tweets without
    being logged in on Sina Weibo, the first search will require accessing different objects, therefore justifying the
    existence of this function.
    :param _url: the url of the landing page from which we will perform our initial search
    :param _query: the query (or keyword) we wish to research
    :return: False if the initial search was unsuccessful (elements could not be accessed for example), true otherwise
    """
    global DRIVER
    DRIVER.get(_url)

    wait_random_long()

    search_bar = find_element_with_timeout(DRIVER, 20,
                                           "//input[@node-type='searchInput']")  # this is NOT the same input bar we will look for after

    if search_bar is None:
        logging.info("[Sina Weibo search] Could not encounter the search bar on landing page, exiting...")
        return False

    wait_random()
    type_slow(_query, search_bar)
    search_bar.send_keys(Keys.RETURN)  # hit it!

    nav_bar = find_element_with_timeout(DRIVER, 10, "//div[@class='m-main-nav']")

    if nav_bar is None:
        logging.info("[Sina Weibo search] Could not encounter the nav bar after entering query, exiting...")
        return False

    categories = find_elements_with_timeout(nav_bar, 10, "//a[@href]")

    if categories is None:
        logging.info("[Sina Weibo search] Could not encounter the latest search bar after entering query, exiting...")
        return False

    for element in categories:
        href = element.get_attribute("href")
        if "realtime" in href:  # this is what we are looking for
            logging.info(f"[Sina Weibo search] Navigating to new query: {href}")
            element.click()
            break

    return True


def proceed_to_next_keyword(_query, _chars_in_last_keyword):
    """
    Once the initial search complete, navigating to the next keyword we wish to search is far simpler. This function can
    be called multiple times once the initial search is done to pass the next keyword.
    :param _query: the keyword that we are looking for
    :param _chars_in_last_keyword: the number of characters in the last keyword we searched (the number of times we will
    need to hit "backspace" to remove these characters organically in our search bar)
    :return: False if the search was unsuccessful (elements could not be accessed for example), true otherwise
    """
    global DRIVER
    search_bar = find_element_with_timeout(DRIVER, 20,
                                           "//input[@class='woo-input-main']")  # the bar we will be looking for AFTER the first search

    if YIELDED_ITEMS >= MAXIMUM_ITEMS_TO_COLLECT:
        logging.info(f"[Sina Weibo] proceed_to_next_keyword - Stopping.")      
        return False  # Stop the generator if the maximum number of items has been reached
    if search_bar is None:
        logging.info("Could not navigate to proper URL, exiting...")
        return False

    wait_random()
    for i in range(0, _chars_in_last_keyword):
        search_bar.send_keys(Keys.BACKSPACE)  # delete last keyword
        wait_random_short()

    wait_random()
    type_slow(_query, search_bar)
    search_bar.send_keys(Keys.RETURN)  # hit it!

    DRIVER.switch_to.window(DRIVER.window_handles[
                                len(DRIVER.window_handles) - 1])  # inputing a new request in sina's search bar creates a new tab

    nav_bar = find_element_with_timeout(DRIVER, 10, "//div[@class='m-main-nav']")

    if nav_bar is None:
        logging.info("[Sina Weibo process] Could not encounter the nav bar after entering query, exiting...")
        return False

    categories = find_elements_with_timeout(nav_bar, 10, "//a[@href]")

    if categories is None:
        logging.info("[Sina Weibo process] Could not encounter the latest search bar after entering query, exiting...")
        return False

    for element in categories:        
        if YIELDED_ITEMS >= MAXIMUM_ITEMS_TO_COLLECT:
            logging.info(f"[Sina Weibo] proceed_to_next_keyword loop - Stopping.")      
            break  # Stop the generator if the maximum number of items has been reached
        href = element.get_attribute("href")
        if "realtime" in href:  # this is what we are looking for
            logging.info(f"[Sina Weibo process] Navigating to new query: {href}")
            element.click()
            break

    return True


def scroll_collect():
    """
    Scroll down the page, up to 10 elements will be displayed without being logged in on Sina Weibo
    :return: the card elements (up to 10) that were loaded on the page after scrolling
    """
    global DRIVER

    smooth_scrolling()  # scroll smoothly all the way to the end of the page

    all_cards = DRIVER.find_elements(By.XPATH, "//div[@class='card']")  # get all the cards

    return all_cards


def reconstruct_time_stamp(_publish_time: str):
    """
    Reconstruct a standard timestamp from the chinese structure proposed by Sina Weibo
    :param _publish_time: the publish time, normally under the form of "X seconds ago" or "Y minutes ago"
    :return: a standard datetime format if the post was <= MAX_POST_AGE_IN_MINUTES and None otherwise
    """
    if SECONDS_AGO in _publish_time:
        seconds_since_post = int(_publish_time.split(SECONDS_AGO)[0])
        date = datetime.utcnow() - timedelta(hours=0, minutes=0, seconds=seconds_since_post)
        return date.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    elif MINUTES_AGO in _publish_time:
        minutes_since_post = int(_publish_time.split(MINUTES_AGO)[0])
        if minutes_since_post > MAX_POST_AGE_IN_MINUTES:  # post is too old, skip it
            return None
        date = datetime.utcnow() - timedelta(hours=0, minutes=minutes_since_post)
        return date.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    return None

def clean_content(content):
    content = ''.join(ch for ch in content if ch < '\uE000' or ch > '\uF8FF')
    return content.replace('#', ' ')

async def process_and_send(_all_cards):
    """
    Asynchronous function to process every card and output data
    :param _all_cards: the cards containing all the items for the specified keyword
    :return:yield an item with all the relevant information
    """

    """
    <div class="card">
        <div class="info"></div> : username
        <div class="from">
            <a href=[link]/> : publish time of the post (time since the post was released)
        </div>
        <p class="txt"/> : content of the post
    </div>
    """
    global CONSECUTIVE_OLD_COMMENTS_COUNT, MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS

    for card in _all_cards:
        try:
            logging.debug(f"Max Consecutive old comments  = {MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS}, current count = {CONSECUTIVE_OLD_COMMENTS_COUNT}")
            if CONSECUTIVE_OLD_COMMENTS_COUNT >= MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS:
                break            
            if YIELDED_ITEMS >= MAXIMUM_ITEMS_TO_COLLECT:
                logging.debug(f"[Sina Weibo] process_and_send - Stopping.")      
                break  # Stop the generator if the maximum number of items has been reached

            username = card.find_element(By.XPATH, ".//a[@nick-name]").text
            content = card.find_element(By.XPATH, ".//p[@class='txt']").text

            container = card.find_element(By.XPATH, ".//div[@class='from']")
            container2 = container.find_elements(By.XPATH, ".//a[@href]")

            post_url = None
            publish_time = None

            for element in container2:
                ref = element.get_attribute("href")
                if "refer_flag" in ref:
                    post_url = ref
                    publish_time = reconstruct_time_stamp(element.text)
                    break

            if publish_time is None or post_url is None:
                logging.info("[Sina Weibo data] Skipping comment as it is too old...")
                CONSECUTIVE_OLD_COMMENTS_COUNT += 1                
                continue
            else:
                CONSECUTIVE_OLD_COMMENTS_COUNT = 0 # reset the count if we found a recent post

            content = clean_content(content) # filtering weird chars

            ##### Forge item
            ## start with hash of author
            sha1 = hashlib.sha1()
            # Update the hash with the author string encoded to bytest
            author = "anonymous"
            try:
                author = username
            except:
                pass
            sha1.update(author.encode())
            author_sha1_hex = sha1.hexdigest()
            logging.info(f"[Sina Weibo data] Author: {author_sha1_hex}")
            logging.info(f"[Sina Weibo data] Content (chinese): {content}")
            logging.info(f"[Sina Weibo data] Post URL: {post_url}")
            logging.info(f"[Sina Weibo data] Post creation time: {publish_time}")
            yield Item(
                content=Content(content),
                author=Author(author_sha1_hex),
                created_at=CreatedAt(publish_time),
                url=Url(post_url),
                domain=Domain("weibo.com"))
        except Exception as e:
            logging.info(f"[Sina Weibo ERROR] {e}")
            pass


#############################################################################
#############################################################################
#############################################################################
#############################################################################
#############################################################################


DEFAULT_OLDNESS_SECONDS = 350
DEFAULT_MAXIMUM_ITEMS = 40
DEFAULT_MIN_POST_LENGTH = 25
DEFAULT_KEYWORDS = ["比特币", "以太坊", "ETH", "crypto", "BTC", "USDT", "加密货币", "索拉纳", "狗狗币", "卡尔达诺", 
        "门罗币", "波卡", "瑞波币", "XRP", "稳定币", "DeFi", "中央银行数字货币", "纳斯达克", 
        "标普500", "标普500", "BNB", "交易所交易基金", "现货ETF", "比特币ETF", "加密", "山寨币", 
        "DeFi", "GameFi", "NFT", "NFTs", "加密货币", "加密", "Twitter限制", "数字", "空投", 
        "金融", "流动性","代币", "经济", "市场", "股票", "危机", "俄罗斯", "战争", "乌克兰" "奢侈", 
        "LVMH", "埃隆·马斯克", "冲突", "银行", "詹斯勒", "骚乱", "FaceID", "暴乱", "法国暴乱", "法国", "Louis Vuitton", "Ralph Lauren",
         "Dior", "Channel",   "美国", "USA", "中国", "德国", "欧洲", "欧洲联盟(EU)", "加拿大", "墨西哥", "巴西", "价格", "市场", 
        "纽约证券交易所","纳斯达克", "CAC", "CAC40", "G20", "石油价格", "富时", "纽约证券交易所",
         "华尔街", "货币", "外汇", "交易", "货币", "美元", "沃伦·巴菲特", "黑石", "伯克希尔", "首次公开募股", "苹果", "特斯拉","Alphabet (GOOG)", "FB股票","债务",
         "比特幣", "俄罗斯", "中国", "以太坊", "美国", "法國", "德國", "英國", "加密貨幣", "代幣", "日本", "烏克蘭", "習近平",
                    "拜登", "普京", "金融", "馬克龍", "穩定幣", "泰達幣", "幣安"]
DEFAULT_URL = "https://weibo.com/login.php"
DEFAULT_NUMBER_CONSECUTIVE_OLD_COMMENTS = 8

def read_parameters(parameters):
    global MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS, CONSECUTIVE_OLD_COMMENTS_COUNT
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

        try:
            keywords = parameters.get("keywords", DEFAULT_KEYWORDS)
        except KeyError:
            keywords = DEFAULT_KEYWORDS
        try:
            url = parameters.get("url", DEFAULT_URL)
        except KeyError:
            url = DEFAULT_URL
        try:
            MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS = parameters.get("max_consecutive_old_posts", DEFAULT_NUMBER_CONSECUTIVE_OLD_COMMENTS)
        except KeyError:
            MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS = DEFAULT_NUMBER_CONSECUTIVE_OLD_COMMENTS
    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        keywords = DEFAULT_KEYWORDS
        url = DEFAULT_URL
        MAX_NUMBER_CONSECUTIVE_OLD_COMMENTS = DEFAULT_NUMBER_CONSECUTIVE_OLD_COMMENTS

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, keywords, url


############################################################################################################################

def is_within_timeframe_seconds(dt_str : str, timeframe_sec: int):
    # Convert the datetime string to a datetime object
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")

    # Make it aware about timezone (UTC)
    dt = dt.replace(tzinfo=timezone.utc)

    # Get the current datetime in UTC
    current_dt = datett.now(timezone.utc)

    # Calculate the time difference between the two datetimes
    time_diff = current_dt - dt

    # Check if the time difference is within the specified timeframe in seconds
    if abs(time_diff) <= timedelta(seconds=timeframe_sec):
        return True
    else:
        return False

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    """
    Perform asynchronous queries on the specified list of keywords one at a time and yield results as soon as they are collected
    :param _url: the initial url from which we will begin our user journey
    :param _keywords: the list of keywords we wish to itereate through
    :return: asynchronously yields the results per keyword. Starts with an initial search than moves on to the next keywords automatically
    """
    global DRIVER, YIELDED_ITEMS, MAXIMUM_ITEMS_TO_COLLECT

    max_oldness_seconds, MAXIMUM_ITEMS_TO_COLLECT, min_post_length, _keywords, _url = read_parameters(parameters)
    YIELDED_ITEMS = 0  # Counter for the number of yielded items
    consecutive_rejected_items = 8

    if "weibo.com" not in _url:
        raise ValueError("Not a Sina Weibo URL")

    init_driver()  # init the driver
    try:
        if start_search(_url, _keywords[0]):  # navigate through the landing page to the first element that interests us (different layout)
            async for item in process_and_send(
                    scroll_collect()):  # scroll through the page to collect all the elements relevant to us                 
                if YIELDED_ITEMS >= MAXIMUM_ITEMS_TO_COLLECT:
                    logging.info(f"[Sina Weibo] Stopping now.")      
                    break  # Stop the generator if the maximum number of items has been reached
                ### YIELDED ITEM                                                
                if  is_within_timeframe_seconds(dt_str=item['created_at'], timeframe_sec=max_oldness_seconds) \
                    and item['content'] is not None and len(item['content']) >= min_post_length:
                    YIELDED_ITEMS += 1  # Increment the counter for yielded items          
                    logging.info(f"[Sina Weibo] found {YIELDED_ITEMS} new posts..")      
                    yield item
                else:                                
                    consecutive_rejected_items -= 1
                    if consecutive_rejected_items <= 0:
                        break

            if len(_keywords) > 1:
                for i in range(1, len(_keywords)):
                    if consecutive_rejected_items <= 0 or YIELDED_ITEMS:
                        break
                    if proceed_to_next_keyword(_keywords[i], len(
                            _keywords[i - 1])):  # navigate to the next keyword using the existing search bar
                        async for item in process_and_send(scroll_collect()):  # append the following items to the list   
                            ####                                         
                            if YIELDED_ITEMS >= MAXIMUM_ITEMS_TO_COLLECT:
                                break  # Stop the generator if the maximum number of items has been reached
                            ### YIELDED ITEM                                                
                            if  is_within_timeframe_seconds(dt_str=item['created_at'], timeframe_sec=max_oldness_seconds) \
                                and item['content'] is not None and len(item['content']) >= min_post_length:
                                YIELDED_ITEMS += 1  # Increment the counter for yielded items
                                logging.info(f"[Sina Weibo] found {YIELDED_ITEMS} new posts..")     
                                yield item
                            else:                                
                                consecutive_rejected_items -= 1
    except Exception as e:
        logging.info(f"[Sina Weibo Query Error]: {e}")
    finally:
        logging.info("[Sina Weibo] Close driver")
        DRIVER.close()