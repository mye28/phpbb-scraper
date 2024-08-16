#!/usr/bin/env python3

#
# Multi-process and Multi-threaded fast scraper for phpBB forums
#
# Author: 2020, Anton D. Kachalov <mouse@ya.ru>
# License: MIT
#
# TODO:
#  - Support for user login
#

import os
import re
import sys
import time
import orjson
import getopt
import locale
import logging
import logging.handlers
import multiprocessing
import urllib3
import requests
import datetime
import dateutil.parser
from queue import Empty
import multiprocessing as mp
from bs4 import BeautifulSoup

scraper_opts = {
  'parser': 'lxml',
#  'parser': 'html.parser',
  'headers': {},
  'output': None,
  'log_level': logging.WARNING,
  'log_file': None,
  'lc_time': ('ru_RU', 'utf-8'),
  'parse_date': False,
  'save_media': False,
  'save_attachments': False,
  'save_users': False,
  'force': 0,
  'max_workers': 10,
  'pool_size': 1,
  'max_retries': 3,
  'timeout': (0.5, 15.0),
  'passwords': {'f': {}, 't': {}, 'u': None}
}


def send_worker(r):
  retry = 0
  while retry < scraper_opts['max_retries']:
    try:
      req = r.request()
      return req, req.obj
    except requests.exceptions.ConnectionError as e:
      retry += 1
    except requests.exceptions.InvalidSchema as e:
      logging.debug('InvalidSchema: {}'.format(e))
      return None, None
    except requests.exceptions.MissingSchema as e:
      logging.debug('MissingSchema: {}'.format(e))
      return None, None
    except IndexError as e:
      logging.error('Index error: {}'.format(e))
      logging.error('  URL: {}'.format(r))
      sys.exit(1)

  # Only log if r has a url property and that property ends with .html
  if hasattr(r, 'url') and r.url.endswith('.html'):
    logging.debug('After {} requests failed to fetch {}'.format(retry, r))
  else:
    logging.warning('After {} requests failed to fetch {}'.format(retry, r))
  return None, None

def scrape_worker(args):
  ok, obj, r, page_merger = args
  if obj is None:
    return []

  if not ok:
    #logging.warning('Failed to fetch {}'.format(obj))
    return []
  
  pages = obj.scrape(r, page_merger)

  if len(pages) == 0:
    #logging.error('0 pages scraped from {}'.format(obj))
    return None

  logging.debug('Scraped {} pages from {}'.format(len(pages), obj))
  return pages


class PageMerger:
  def __init__(self):
    self._pages = mp.Manager().dict()
    self._lock = mp.Manager().Lock()


  def add(self, opts, session, paths, key, cls, data, size):
    self._lock.acquire()
    self._pages[key] = {
      'data': [None]*size,
      'remain': size,
      'opts': opts,
      'paths': paths,
      'session': session,
      'class': cls,
    }
    self._lock.release()
    return self.append(key, data, 0)


  def append(self, key, data, offset):
    self._lock.acquire()
    v = self._pages[key]
    v['data'][offset:offset+len(data)] = data
    v['remain'] -= len(data)
    # To induce shared dict update need an assignment operation
    self._pages[key] = v
    media = []
    if v['remain'] <= 0:
      media = v['class'].save(
        session=v['session'], id=key,
        data=v['data'], paths=v['paths'],
        opts=v['opts'])
      del self._pages[key]

    self._lock.release()
    return media


  def stats(self):
    self._lock.acquire()
    stats = []
    for k, v in self._pages.items():
      stats.append({
        'remain': v['remain'],
        'size': len(v['data']),
        'paths': ' / '.join([p[0] for p in v['paths']] + [str(k)]),
      })

    self._lock.release()
    return stats


  def force_save(self):
    self._lock.acquire()
    media = []
    if len(self._pages):
      logging.info('FORCE: Saving unmerged pages')
    for key, v in self._pages.items():
      v['data'] = list(filter(None, v['data']))
      media.extend(v['class'].save(session=v['session'], id=key, data=v['data'], paths=v['paths'], opts=v['opts']))
    self._lock.release()
    return media

class FileSaver:
  def __init__(self, opts, session, paths, fname, url, use_session=False):
    self._opts = opts
    self._session = session
    self._paths = paths
    self._fname = fname
    self._url = url
    if not use_session:
      self._session = requests.Session()
      a = requests.adapters.HTTPAdapter(max_retries=self._opts['max_retries'], )
      self._session.mount('http://', a)
      self._session.mount('https://', a)
      self._session.verify = False
      self._opts['headers'] = {}


  def __str__(self):
    return r'FileSaver<{} @ {}>'.format(self._fname, self._url)

  def __repr__(self):
    return self.__str__()

  def request(self):
    #logging.debug('START FileSaver.request: timeout={}, {}'.format(self._opts['timeout'], self._url))
    r = self._session.get(self._url, timeout=self._opts['timeout'])
    r.obj = self
    if r.status_code != 200:
      logging.debug('END Failed to fetch {}: {}'.format(self._url, r.status_code))
      return r
    #logging.debug('END FileSaver.request: Type = {}, {}'.format(r.headers['Content-Type'], self._url))
    return r


  @staticmethod
  def full_path(output, paths, fname):
    fname = re.sub(r'[/\\|:]', '_', fname).encode('utf-8', 'ignore').decode('utf-8')

    ospaths = [p[0] for p in paths]
    dname = os.path.join(output, *ospaths)
    ext = fname.rsplit('.', 1)
    if len(ext) != 2:
      ext = [ext[0], 'unk']
    return '{}.{}'.format(os.path.join(dname, ext[0]), ext[1])


  def scrape(self, resp, page_merger):
    logging.debug('FileSaver.scrape: {} [{} bytes]'.format(self._url, len(resp.content)))
    
    fname = FileSaver.full_path(self._opts['output'], self._paths, self._fname)

    #logging.debug('Saving {} [{} bytes]'.format(fname, len(resp.content)))
    #logging.debug('  Saving {} [{} bytes, {}]'.format(fname, len(resp.content), resp.headers['Content-Type']))

    dname = os.path.dirname(fname)

    try:
      os.makedirs(dname, exist_ok=True)
    except Exception as e:
      logging.error('Error creating directory {}: {}'.format(dname, e))
      return []

    try:
      with open(fname, 'wb+') as f:
        f.write(resp.content)
    except Exception as e:
      logging.error('Unable to save file {}: {}'.format(fname, e))
      return []

    #logging.debug('  Saved {} [{} bytes, {}]'.format(fname, len(resp.content), resp.headers['Content-Type']))

    return []


class PhpBBElement:
  def __init__(self, opts):
    self._opts = opts


  def __str__(self):
    return r'<{}>'.format(self.__class__)

  def __repr__(self):
    return self.__str__()

  def _get_url_query(self, url):
    if isinstance(url, str):
      url = requests.utils.urlparse(url)

    if '=' not in url.query:
      return {}

    return dict(x.split('=') for x in url.query.split('&'))


  def _is_password_required(self):
    f = self._page.select('form#login_forum')
    if not f:
      return False

    return len(f[0].select('input#password')) > 0


  def _pagination(self):
    

    self._path = []

    breadcrumbs = self._page.select('#nav-breadcrumbs > li.breadcrumbs > span.crumb[data-forum-id]')
    for b in breadcrumbs:
      self._path.append((b['data-forum-id'], b.a.span.string.strip()))

    pagination = self._page.select('div.action-bar.bar-top > div.pagination > ul > li:not(.next):not(.page-jump) > a.button')
    url = ''
    pages_count = 1
    start_value = 0
    if pagination:
      pages_count = int(pagination[-1].string.strip())
      url = self._get_url_query(pagination[-1]['href'])
      starts = set([0])
      for p in pagination:
        try:
          u = self._get_url_query(p['href'])
        except Exception as e:
          logging.info('Unable to parse URL {} : {}'.format(p['href'], e))
          continue
        if 'start' in u:
          starts.add(int(u['start']))
      starts = sorted(starts)
      if len(starts) > 1:
        deltas = set()
        for i in range(1, len(starts)):
          deltas.add(starts[i] - starts[i-1])
        start_value = min(deltas)

    if len(self._page.select('div.action-bar.bar-top > div.pagination')) == 0:
      return url, 0, 0
    
    pgs = self._page.select('div.action-bar.bar-top > div.pagination')[0]

    try:
      pgs.ul.decompose()
      pgs.a.decompose()
    except:
      pass
    m = re.search(r'^[^\d+]*(\d+)', pgs.text.strip())
    self._elements_count = int(m.group(1))

    if pages_count == 1:
      start_value = self._elements_count

    return url, start_value, pages_count


class PhpBBForumPassword(PhpBBElement):
  def __init__(self, opts, session, page, password):
    super().__init__(opts)
    self._session = session
    f = page.select('form#login_forum')
    self._url = '{}/{}'.format(self._opts['url'], f[0]['action'])
    url = self._get_url_query(self._url)
    self._opts['sid'] = url['sid']
    self._topic_id = 0
    if 't' in url:
      self._topic_id = int(url['t'])

    self._params = {
      'password': password,
      'login': 'Login',
    }
    for i in f[0].select('input[type="hidden"]'):
      self._params[i['name']] = i['value']

    
    #logging.debug('PhpBBForumPassword.__init__: {}'.format(self._url))

  def __str__(self):
    return r'PhpBBForumPassword<{}>'.format(self._url)


  def request(self):
    r = requests.post(self._url, headers=self._opts['headers'], data=self._params, timeout=self._opts['timeout'], verify=False)
    r.obj = self
    return r


  def scrape(self, resp, page_merger):
    

    self._page = BeautifulSoup(resp.content, self._opts['parser'])

    logging.debug('PhpBBForumPassword.scrape: {}'.format(self._page.title.string))

    msg = self._page.select('#message p')
    if msg:
      logging.error('Login to forum {} failed: {}'.format(self._params['f'], msg[0].string.strip()))
      return []

    msg = self._page.select('form#login')
    if msg:
      logging.error('Login required to scrape forum {}'.format(self._params['f']))
      return []

    if self._is_password_required():
      logging.error('Wrong password provided for forum {}'.format(self._params['f']))
      return []

    if 'viewforum.php' in self._url:
      f = PhpBBForum(self._opts, self._session, self._params['f'], 0)
      return f.scrape(resp, page_merger)

    f = PhpBBTopic(self._opts, self._session, self._topic_id, 0)
    return f.scrape(resp, page_merger)


class PhpBBForum(PhpBBElement):
  def __init__(self, opts, session, forum_id, start):
    super().__init__(opts)
    self._session = session
    self._forum_id = int(forum_id)
    self._start = int(start)
    
    logging.debug('PhpBBForum.__init__: {}'.format(self._url(forum_id=self._forum_id, start=self._start)))

  def __str__(self):
    return r'PhpBBForum<id={} @ {}>'.format(self._forum_id, self._start)


  def request(self):
    r = self._session.get(self._url(forum_id=self._forum_id, start=self._start), timeout=self._opts['timeout'])
    r.obj = self
    return r


  def scrape(self, resp, page_merger):
    
    logging.debug('PhpBBForum.scrape: URL = {}'.format(self._url(forum_id=self._forum_id, start=self._start)))
    self._page = BeautifulSoup(resp.content, self._opts['parser'])

    msg = self._page.select('#message p')
    if msg:
      logging.error('Fetch forum {} failed: {}'.format(self._forum_id, msg[0].string.strip()))
      return []

    msg = self._page.select('form#login')
    if msg:
      logging.error('Login required to scrape forum {}'.format(self._forum_id))
      return []

    if self._is_password_required():
      if not self._forum_id in self._opts['passwords']['f']:
        logging.error('Password required to scrape forum {}'.format(self._forum_id))
        return []
      return [PhpBBForumPassword(self._opts, self._session,
                                 self._page, self._opts['passwords']['f'][self._forum_id])]

    pages = []
    if self._forum_id == 0:
    # Queue forums list only
      for f in self._page.select('div#page-body > div.forabg li.row > dl > dt a.forumtitle'):
        fid = self._get_url_query(f['href'])['f']
        pages.append(PhpBBForum(self._opts, self._session, fid, 0))
      return pages

    #Subforums
    # for f in self._page.select('div#page-body > div.forabg li.row > dl > dt a.forumtitle'):
    #   fid = self._get_url_query(f['href'])['f']
    #   pages.append(PhpBBForum(self._opts, self._session, fid, 0))

    for f in self._page.select('div#page-body > div.forumbg li.row > dl > dt a.topictitle'):
      tid = int(self._get_url_query(f['href'])['t'])
      if tid not in self._opts['scraped_topics']:
        logging.debug('Queue forum {} topic {}'.format(self._forum_id, tid))
        pages.append(PhpBBTopic(self._opts, self._session, tid, 0))

    if self._start != 0:
      return pages

    url, start_value, pages_count = self._pagination()

    if not 'start' in url:
      return pages

    last = int(url['start'])
    for i in range(1, pages_count):
      pages.append(PhpBBForum(self._opts, self._session,
                              forum_id=self._forum_id,
                              start=i * start_value))

    return pages


  def _url(self, forum_id = 0, start = 0):
    args = []
    if 'sid' in self._opts:
      args.append('sid={}'.format(self._opts['sid']))
    if forum_id:
      args.append('f={}'.format(forum_id))
    if start:
      args.append('start={}'.format(start))

    if args:
      return '{}/viewforum.php?{}'.format(self._opts['url'], '&'.join(args))
    return '{}'.format(self._opts['url'])


class PhpBBTopic(PhpBBElement):
  def __init__(self, opts, session, topic_id, start):
    super().__init__(opts)
    self._session = session
    self._posts = []
    self._forum_id = None
    self._topic_id = int(topic_id)
    self._start = int(start)    

  def __str__(self):
    return r'PhpBBTopic<{}>'.format(self._url(self._url(topic_id=self._topic_id, start=self._start)))

  def request(self):
    r = self._session.get(self._url(topic_id=self._topic_id, start=self._start), timeout=self._opts['timeout'])
    r.obj = self
    return r

  def _parse_page(self):
    
    
    for p in self._page.select('div.post'):

      pid = p['id'][1:]
      post = {
       'msg_id': int(pid)
      }
      member = p.select('div > dl.postprofile > dt > a')
      if member:
        post['uid'] = int(self._get_url_query(member[0]['href'])['u'])
        post['user'] = member[0].string.strip()
      date = p.select('#post_content{} > p.author'.format(pid))[0]
      # HACK: find(recursive=False, text=True) doesn't work
      date.a.decompose()
      date.span.decompose()
      post['date'] = date.text.strip()
      if self._opts['parse_date']:
        post['date'] = int(dateutil.parser.parse(post['date']).timestamp())

      content = p.select('#post_content{} > div.content'.format(pid))[0]

      bbContent = self._html2bb(content)
      if bbContent:
        post.update(bbContent)
      else:
        logging.error('Failed to parse post {}. Url = {}'.format(pid, self._url(topic_id=self._topic_id, start=self._start, post=pid)))
        return False

        continue

      for img in p.select('dl.attachbox img.postimage'):
        post['files'].append((img['alt'], '{}/{}'.format(self._opts['url'], img['src'])))
        img.decompose()

      post ['subject'] = self._topic_title
      post ['topic_id'] = self._topic_id
      post ['forum'] = self._forum_id

      self._posts.append(post)
    return True


  def _unwrap_tag(self, tag, before, after, extract=None, cleanup=None):
    if extract:
      if not isinstance(extract, list):
        extract = [extract]
      v = [x(tag) for x in extract]
      if '{}' in before:
        before = before.format(*v)
      if '{}' in after:
        after = after.format(*v)

    if before:
      tb = self._page.new_tag('span')
      tb.string = before
      tag.insert_before(tb)
      tb.unwrap()

    if after:
      ta = self._page.new_tag('span')
      ta.string = after
      tag.insert_after(ta)
      ta.unwrap()

    if cleanup:
      cleanup(tag)

    tag.unwrap()


  def _replace_tags(self, tags, before, after, extract=None, cleanup=None):
    for t in tags:
      self._unwrap_tag(t, before, after, extract, cleanup)


  def _extract_style(self, tag, name):
    style = tag['style']
    if ';' in style:
      style = style.split(';')
    else:
      style = [style]
    for s in style:
      k,v = s.strip().split(':', 1)
      if k.strip() == name:
        return v.strip()

    return None      

  def _html2bb(self, content):

    res = {'files': []}
    if self._opts['save_media']:
      res['media'] = []

    try:
      for img in content.select('div.inline-attachment > dl.thumbnail img.postimage'):
        # HACK: drop '../' from URL
        url = img['src'].replace('../', '')
        res['files'].append((img['alt'], '{}/{}'.format(self._opts['url'], url)))
        img.decompose()

      for a in content.select('div.inline-attachment > dl.file a.postlink'):
        # HACK: drop '../' from URL
        url = a['href'].replace('../', '')
        res['files'].append((a.string.strip(), '{}/{}'.format(self._opts['url'], url)))
        a.decompose()

      # Drop "Edit by" notice message
      for c in content.select('div.notice'):
        c.decompose()

      # Drop buttons from spoiler div
      for c in content.select('div > input.button2'):
        c.decompose()

      # Drop signatures
      for c in content.select('div.signature'):
        c.decompose()

      # Unwrap [code] section
      for c in content.select('div.codebox'):
        c.p.decompose()
        c.pre.code.unwrap()
        c.pre.unwrap()
        cp = self._page.new_tag('pre')
        c.wrap(cp)
        c.unwrap()
        if cp.string:
          cp.string = cp.string.replace('\n', '&#10;')

      self._replace_tags(content.find_all('pre'), '[code]', '[/code]')

      self._replace_tags(content.find_all('br'), '&#10;', '')
      self._replace_tags(content.select('span[style*="text-decoration:underline"]'), '[u]', '[/u]')
      self._replace_tags(content.select('span[style*="color:"]'), '[color={}]', '[/color]',
        lambda tag: self._extract_style(tag, 'color'))
      self._replace_tags(content.find_all('strong'), '[b]', '[/b]')
      self._replace_tags(content.select('em.text-italics'), '[i]', '[/i]')

      self._replace_tags(content.find_all('li'), '[*]', '')
      self._replace_tags(content.select('ol[style*="list-style-type:lower-alpha"]'), '[list=a]', '[/list]')
      self._replace_tags(content.select('ol[style*="list-style-type:decimal"]'), '[list=1]', '[/list]')
      self._replace_tags(content.select('a.postlink'), '[url{}]', '[/url]',
        lambda tag: '' if tag['href'] == tag.string else '={}'.format(tag['href']))
      self._replace_tags(content.select('img.smilies'), '{}', '',
        lambda tag: tag['alt'])

      if self._opts['save_media']:
        for i in content.select('img.postimage'):
          if len(i['src']) == 0:
            #logging.error('img.postimage: Empty src in {} (url = {})'.format(i, self._topic_id))
            continue
          if i['src'][0] == '.':
            i['src'] = '{}/{}'.format(self._opts['url'], i['src'])

          # remove query string from URL
          i['src'] = i['src'].split('?', 1)[0]
          i['src'] = i['src'].split('&', 1)[0]

          res['media'].append((os.path.basename(i['src']), i['src']))

        for i in content.select('img[height]'):
          if len(i['src']) == 0:
            #logging.error('img[height]: Empty src in {} (url = {})'.format(i, self._topic_id))
            continue
          if i['src'][0] == '.':
            i['src'] = '{}/{}'.format(self._opts['url'], i['src'])

          # remove query string from URL
          i['src'] = i['src'].split('?', 1)[0]
          i['src'] = i['src'].split('&', 1)[0]
          res['media'].append((os.path.basename(i['src']), i['src']))

      self._replace_tags(content.select('img.postimage'), '[img]{}', '[/img]', lambda tag: tag['src'])
      self._replace_tags(content.select('img[height]'), '[fimg={}]{}', '[/fimg]', [lambda tag: tag['height'], lambda tag: tag['src']])

      self._replace_tags(content.find_all('ul'), '[list]', '[/list]')
      self._replace_tags(content.select('div[style*="padding:"]'), '[spoiler={}]', '[/spoiler]',
        lambda tag: tag.span.text.strip().replace(']', '\\]'),
        lambda tag: tag.span.decompose())
      self._replace_tags(content.select('span[style*="font-size:"]'), '[size={}]', '[/size]',
        lambda tag: self._extract_style(tag, 'font-size')[:-1])

      quotes = content.find_all('blockquote')
      quotes.reverse()
      for quote in quotes:
        q = {'who': ''}
        if quote.cite:
          if not quote.cite.a:
            if quote.cite.string:
              q['who'] = '={}'.format(quote.cite.string.strip().rsplit(' ', 1)[0].replace(']', '\\]'))
            elif quote.cite.text:
              # A url?
              q['who'] = '={}'.format(quote.cite.text)
          else:
            uq = self._get_url_query(quote.cite.a['href'])
            if 'u' in uq:
              q['who'] = '={} user_id={}'.format(
                quote.cite.a.string.strip().replace(']', '\\]'),
                uq['u'])
            else:
              q['who'] = '={}'.format(quote.cite.a.string.strip().replace(']', '\\]'))
          pt = quote.cite.select('div.responsive-hide')
          if pt:
            qdate = pt[0].string.strip()
            if self._opts['parse_date']:
              qdate = int(dateutil.parser.parse(qdate).timestamp())
            q['who'] += ' time={}'.format(qdate)
          quote.cite.decompose()

        quote.div.unwrap()
        self._unwrap_tag(quote, '[quote{}]'.format(q['who']), '[/quote]')

      res['content'] = content.text.replace('\n', '').replace('&#10;', '\n')

    except Exception as e:
      
      logging.error('_html2bb Exception: {}'.format(e))
      logging.error(' URL = {}'.format(self._url(topic_id=self._topic_id, start=self._start)))
      return None

    return res


  def _url(self, topic_id = 0, start = 0, post = 0):
    args = []
    if 'sid' in self._opts:
      args.append('sid={}'.format(self._opts['sid']))
    if topic_id:
      args.append('t={}'.format(topic_id))
    if start:
      args.append('start={}'.format(start))
    if post:
      args.append('p={}'.format(post))

    return '{}/viewtopic.php?{}'.format(self._opts['url'], '&'.join(args))


  def scrape(self, resp, page_merger):
    self._page = BeautifulSoup(resp.content, self._opts['parser'])

    #logging.debug('PhpBBTopic.scrape: {}'.format(self._url(topic_id=self._topic_id, start=self._start)))

#    self._page = BeautifulSoup('<html><body><blockquote><cite>asd]bbb</cite><div>LLLL</div></blockquote><div class="codebox"><p>KOD</p><pre><code>TEST CODE\nFFFF</code></pre></div><br/><span style="text-decoration:underline">UNDERLINE</span>\n\n<ul><li>ITEM1</li></ul><ol style="list-style-type:lower-alpha"><li>ITEM1</li></ol><ol style="list-style-type:decimal"><li>ITEM2</li></ol><img src="https://avatars.mds.yandex.net/get-banana/55914/x25Bic0D9kVbOCgUbeLnDbwof_banana_20161021_22_ret.png/optimize" height="200"><img src="https://avatars.mds.yandex.net/get-banana/55914/x25Bic0D9kVbOCgUbeLnDbwof_banana_20161021_22_ret.png/optimize"><a href="http://localhost/" class="postlink">http://localhost/</a><span style="color:red">RED</span><span style="font-size: 50%; line-height: normal">SMALL</span>&lt;aaa;&gt;&eacute;</body></html>', self._opts['parser'])
#    self._html2bb(self._page)

    msg = self._page.select('#message p')
    if msg:
      logging.error('Fetch {} failed: {}'.format(self._topic_id, msg[0].string.strip()))
      return []

    msg = self._page.select('form#login')
    if msg:
      logging.error('Login required to scrape {}'.format(self._topic_id))
      return []

    if self._is_password_required():
      if not self._topic_id in self._opts['passwords']['t']:
        logging.error('Password required to scrape topic {}'.format(self._topic_id))
        return []
      return [PhpBBForumPassword(self._opts, self._session,
                                 self._page, self._opts['passwords']['t'][self._topic_id])]

    self._topic_title = self._page.select('div#page-body > h2.topic-title')[0].text.strip()
    self._locked = len(self._page.select('div.action-bar > a > i.fa-lock')) != 0

    url, start_value, pages_count = self._pagination()
    self._forum_id = self._path[-1]
    if self._parse_page() == False:
      return []

    if self._start != 0:
      return page_merger.append(self._topic_id, self._posts, self._start)

    pages = []
    media = page_merger.add(opts=self._opts, session=self._session,
                            paths=self._path, key=self._topic_id,
                            data=self._posts, size=self._elements_count,
                            cls=PhpBBTopic)
    pages.extend(media)
    if not 'start' in url:
      return pages

    last = int(url['start'])
    for i in range(1, pages_count):
      pages.append(PhpBBTopic(self._opts, self._session,
                              topic_id=self._topic_id,
                              start=i * start_value))

    return pages


  @staticmethod
  def save(session, id, data, paths, opts):
    

    ospaths = [p[0] for p in paths]
    dname = os.path.join(opts['output'], *ospaths)
    fname = '{}.json'.format(os.path.join(dname, str(id)))
    logging.info('Saving {} posts to {}'.format(len(data), fname))
    media = []
    if opts['save_media'] or opts['save_attachments']:
      fpaths = paths.copy()
      fpaths.append((str(id), 'files'))
      for d in data:
        if d['files']:
          for i in d['files']:
            fn = FileSaver.full_path(opts['output'], fpaths, i[0])
            if not os.path.isfile(fn) or opts['force'] == 2:
              media.append(FileSaver(opts=opts, session=session, paths=fpaths, fname=i[0], url=i[1], use_session=True))

        if 'media' in d:
          for i in d['media']:
            fn = FileSaver.full_path(opts['output'], fpaths, i[0])
            if not os.path.isfile(fn) or opts['force'] == 2:
              media.append(FileSaver(opts=opts, session=session, paths=fpaths, fname=i[0], url=i[1], use_session=False))
          del d['media']

    try:
      os.makedirs(dname, exist_ok=True)
    except Exception as e:
      logging.error('Error creating directory {}: {}'.format(dname, e))
      return []

    # Dump forum meta if not exists
    mpaths = [opts['output']]
    for p in paths:
      mpaths.append(p[0])
      mname = os.path.join(*mpaths, '_meta.json')
      if os.path.isfile(mname):
        continue
      try:
        with open(mname, 'wb+') as f:
          f.write(orjson.dumps({'id': p[0], 'name': p[1]}))
      except Exception as e:
        logging.error('Unable to create forum metadata {}: {}'.format(mname, e))
        return []

    try:
      with open(fname, 'wb+') as f:
        f.write(orjson.dumps(data))
    except Exception as e:
      logging.error('Error saving posts to {}: {}'.format(fname, e))
      return []

    return media


class PhpBBUsers(PhpBBElement):
  def __init__(self, opts, session, start=0):
    super().__init__(opts)
    self._session = session
    self._start = start
    self._path = []


  def __str__(self):
    return r'PhpBBUsers<start={}>'.format(self._start)


  def request(self):
    r = self._session.get(self._url(self._start), timeout=self._opts['timeout'])
    r.obj = self
    return r


  def scrape(self, resp, page_merger):
    

    logging.info('PhpBBUsers.scrape')
    self._page = BeautifulSoup(resp.content, self._opts['parser'])

    logging.info('PhpBBUsers.scrape: {}'.format(self._page.title.string))
    
    msg = self._page.select('#message p')
    if msg:
      logging.error('Fetch users failed: {}'.format(msg[0].string.strip()))
      return []

    msg = self._page.select('form#login')
    if msg:
      logging.error('Login required to scrape users')
      return []

    users = []
    pages = []
    fpaths = self._path + [('users', 0)]
    for tr in self._page.select('table#memberlist > tbody > tr'):
      tds = tr.find_all('td')
      u = tds[0]
      registered = tds[-1]
      rdate = registered.text.strip()
      if self._opts['parse_date']:
        rdate = int(dateutil.parser.parse(rdate).timestamp())
      uid = self._get_url_query(u.a['href'])['u']
      users.append({
        'uid': uid,
        'date': rdate,
        'user': u.a.string.strip()
      })
      if not self._opts['save_attachments'] and not self._opts['save_media']:
        continue

      for ext in ('png', 'jpg'):
        fn='{}.{}'.format(uid, ext)
        fname = FileSaver.full_path(self._opts['output'], fpaths, fn)
        if os.path.isfile(fname) or self._opts['force']:
          continue
        pages.append(FileSaver(opts=self._opts, session=self._session,
                               paths=fpaths, fname=fn,
                               url='{}/download/file.php?avatar={}.{}'.format(self._opts['url'], uid, ext),
                               use_session=True))


    url, start_value, pages_count = self._pagination()

    if self._start != 0:
      pages.extend(page_merger.append('users', users, self._start))
      return pages

    page_merger.add(opts=self._opts, session=self._session,
                    paths=self._path, key='users', data=users,
                    size=self._elements_count, cls=PhpBBUsers)
    if not 'start' in url:
      return pages

    last = int(url['start'])
    for i in range(1, pages_count):
      pages.append(PhpBBUsers(self._opts, self._session, start=i * start_value))

    return pages


  def _url(self, start = 0):
    args = []
    if 'sid' in self._opts:
      args.append('sid={}'.format(self._opts['sid']))
    if start:
      args.append('start={}'.format(start))

    return '{}/memberlist.php?{}'.format(self._opts['url'], '&'.join(args))


  @staticmethod
  def save(session, id, data, paths, opts):
    

    ospaths = [p[0] for p in paths]
    dname = os.path.join(opts['output'], *ospaths)
    fname = '{}.json'.format(os.path.join(dname, str(id)))
    logging.info('Saving {} users to {}'.format(len(data), fname))

    try:
      os.makedirs(dname, exist_ok=True)
    except Exception as e:
      logging.error('Error creating directory {}: {}'.format(dname, e))
      return []

    try:
      with open(fname, 'wb+') as f:
        f.write(orjson.dumps(data))
    except Exception as e:
      logging.error('Error saving posts to {}: {}'.format(fname, e))
      return []

    return []


class RequestsIter:
  def __init__(self, size):
    self._queue = mp.Manager().Queue(size)
    self._enqueued = 0
    self._processed = 0

  def __next__(self):
    try:
      return self._queue.get(block=False)
    except Empty:
      raise StopIteration


  def __iter__(self):
    return self


  def is_done(self):
    if not self._queue.empty():
      return False

    if self._processed < self._enqueued:
      return False

    if self._processed > self._enqueued:
      logging.info('Processed {} pages; more than enqueued {}'.format(self._processed, self._enqueued))
    return True


  def put(self, item):    
    self._enqueued += 1
    #logging.debug('put #{}: {}'.format(self._enqueued, item))
    self._queue.put(item)

  def processed(self):
    self._processed += 1
    #logging.debug('processed #{}'.format(self._processed))

  def reset(self):
    if self._queue._qsize > 0:
      logging.error('Queue is not empty')
      
    self._queue = mp.Manager().Queue(self._queue.maxsize)
    self._enqueued = 0
    self._processed = 0

class PhpBBScraper:
  def __init__(self, opts, forums_arg, topics_arg):
    self._opts = opts
    self._page_merger = PageMerger()
    self._topics = []
    self._processed_pages = 0
    self._opts['scraped_topics'] = []
    self._session = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=self._opts['max_retries'])
    self._session.mount('http://', a)
    self._session.mount('https://', a)
    self._session.verify = False
    self._session.headers.update(opts['headers'])
    self._queue = RequestsIter(-1) #self._opts['pool_size'] * 4)
    if not self._opts['force']:
      self._load_state()

    if self._opts['save_users']:
      self._topics.append(PhpBBUsers(self._opts, self._session))
    elif not forums_arg and not topics_arg:
      forums_arg = ['0']

    self._parse_arg(forums_arg, is_topics=False)
    self._parse_arg(topics_arg, is_topics=True)

  def is_done(self):
    return self._queue.is_done()

  def scrape(self):
    self.enqueue(self._topics)
    logging.debug('PhpBBScraper.scrape: Topics = {}, Enqueued = {}'.format(len(self._topics), self._queue._enqueued))

    self._topics = []

    pool = mp.pool.ThreadPool(processes=self._opts['pool_size'])
    while not self._queue.is_done():
      for response, obj in pool.imap_unordered(send_worker, self._queue):
        #logging.debug ('send_worker done: {} {}'.format(self._queue.processed, obj))
        if response is None:
          logging.debug('send_worker: None response')
          self.processed()
          yield (False, obj, None, None)
          continue

        if isinstance(obj, PhpBBTopic) and obj._start == 0:
          #logging.debug('send_worker: isinstance(obj, PhpBBTopic) and obj._start == 0')
          self._processed_pages += 1
          #logging.debug('Processed pages: {}'.format(self._processed_pages))

        if response.status_code == 200:
          #logging.debug('send_worker: response.status_code == 200')
          # Yield good result, no response needed to pass back
          yield (True, obj, response, self._page_merger)
        else:
          logging.debug('Bad status received: {}, {}'.format(response.status_code, obj))
          # Bad status received, push back the response details
          self.processed()
          yield (False, obj, response, None)

    pool.close()
    pool.join()


  def enqueue(self, pages):
    for p in pages:
      self._queue.put(p)

  def processed(self):
    self._queue.processed()


  def stats(self):
    tm = self._page_merger.stats()
    pages = ['{}\t{} of {}'.format(v['paths'], v['remain'], v['size']) for v in tm]

    logging.info('===== STATS =====')
    if self._opts['scraped_topics']:
      logging.info('     Already Scraped Topics: {}'.format(len(self._opts['scraped_topics'])))
    logging.info('     Processed Requests: {}'.format(self._queue.processed))
    if self._processed_pages:
      logging.info('     Processed Pages: {}'.format(self._processed_pages))
    if pages:
      logging.info('     Hidden Posts:\n\t{}'.format('\n\t'.join(pages)))


  def force_merge(self):
    return self._page_merger.force_save()


  def _parse_arg(self, arg, is_topics):
    for t in arg:
      password = None
      if ':' in t:
        t, password = t.split(':', 1)
      if ',' in t:
        item_list = t.split(',')
      else:
        item_list = [t]

      for r in item_list:
        if '-' not in r:
          r = int(r)
          if password:
            self._opts['passwords']['t' if is_topics else 'f'][r] = password
          if is_topics and r not in self._opts['scraped_topics']:
            self._topics.append(PhpBBTopic(self._opts, self._session, r, 0))
          elif not is_topics:
            self._topics.append(PhpBBForum(self._opts, self._session, r, 0))
          continue

        rlist = r.split('-')
        if len(rlist) != 2:
          raise ValueError('Wrong specifier for topic range in: {}'.format(t))

        for i in range(int(rlist[0]), int(rlist[1])+1):
          if password:
            self._opts['passwords']['t' if is_topics else 'f'][i] = password
          if is_topics and i not in self._opts['scraped_topics']:
            self._topics.append(PhpBBTopic(self._opts, self._session, i))
          elif not is_topics:
            self._topics.append(PhpBBForum(self._opts, self._session, i))


  def _load_state(self):
    
    logging.info('Searching for downloaded topics in {}'.format(self._opts['output']))
    for dname, subdirs, flist in os.walk(self._opts['output']):
      for f in flist:
        if f == '_meta.json':
          continue
        if '.json' in f:
          try:
            tid = int(f[:-5])
            self._opts['scraped_topics'].append(tid)
          except:
            pass
    logging.info('{} topics already scraped'.format(len(self._opts['scraped_topics'])))


def usage(rc):
  print('Usage: {} [-v|-h|-w workers|-p pool-size|-o output-dir|-a user-agent|-c cookie|-m|-s|[-t|-f] id[-id|,id] [-l log-file]] URL\n'.format(os.path.basename(sys.argv[0])))
  sys.exit(rc)


## ---- Logging ----
g_log_level = logging.INFO
g_log_format = '%(asctime)s %(processName)-10s %(levelname)-8s %(message)s'
g_log_file = 'scraper.log'

#
# Because you'll want to define the logging configurations for listener and workers, the
# listener and worker process functions take a configurer parameter which is a callable
# for configuring logging for that process. These functions are also passed the queue,
# which they use for communication.
#
# In practice, you can configure the listener however you want, but note that in this
# simple example, the listener does not apply level or filter logic to received records.
# In practice, you would probably want to do this logic in the worker processes, to avoid
# sending events which would be filtered out between processes.
#
# The size of the rotated files is made small so you can see the results easily.
def listener_configurer(log_level, log_format, log_file):
    root = logging.getLogger()

    if log_file is not None:
      fh = logging.handlers.RotatingFileHandler(log_file, 'a', 30000, 10)
      fh.setFormatter(logging.Formatter(log_format))
      root.addHandler(fh)
    else:
      ch = logging.StreamHandler()
      ch.setFormatter(logging.Formatter(log_format))
      root.addHandler(ch)

    # ch = logging.StreamHandler()
    # ch.setFormatter(logging.Formatter(log_format))

    # if log_file is not None:
    #   ch.setLevel(logging.INFO)
    # else:
    #   ch.setLevel(log_level)

    # root.addHandler(ch)

    #if log_file is not None:


# This is the listener process top-level loop: wait for logging events
# (LogRecords)on the queue and handle them, quit when you get a None for a
# LogRecord.
def listener_process(queue, configurer, log_level, log_format, log_file):
    import sys, traceback
    configurer(log_level, log_format, log_file)
    while True:
        try:
            record = queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                #print('Quit listener_process:', file=sys.stderr)
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

def init_worker(configurer, queue, log_level):
    configurer(queue, log_level)
    # grab the number off the name of the process name (which is in the form "SpawnPoolWorker-N")
    # and set it as the process name ("worker-N").
    multiprocessing.current_process().name="Worker-{}".format(multiprocessing.current_process().name.split("-")[1])

# The worker configuration is done at the start of the worker process run.
# Note that on Windows you can't rely on fork semantics, so each process
# will run the logging configuration code when it starts.
def worker_configurer(queue, log_level):
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(log_level)
    # Disable all child loggers of urllib3, e.g. urllib3.connectionpool
    logging.getLogger("urllib3").propagate = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main():
  # Clean up any extra spaces in the command-line arguments
  args = []
  for arg in sys.argv[1:]:
    if arg.strip():  # Ignore empty strings resulting from spaces
      args.append(arg.strip())

  try:
    opts, args = getopt.getopt(args,
      'hf:t:w:a:c:mso:up:vl:', [
      'help',
      'force',
      'forum=',
      'topic=',
      'workers=',
      'pool-size=',
      'user-agent=',
      'cookie=',
      'save-media',
      'save-attachments',
      'save-users',
      'output=',
      'log-file=',
      'parse-date',
    ])

    scraper_opts['url'] = args[-1]
    
  except getopt.GetoptError as err:
    print(err)
    usage(2)

  if 'url' not in scraper_opts:
    usage(1)

  topics = []
  forums = []

  for o, a in opts:
    if o in ('-h', '--help'):
      usage(0)
    elif o == '--parse-date':
      scraper_opts['parse_date'] = True
    elif o == '--force':
      scraper_opts['force'] += 1
    elif o in ('-f', '--forum'):
      forums.append(a)
    elif o in ('-t', '--topic'):
      topics.append(a)
    elif o in ('-w', '--workers='):
      scraper_opts['max_workers'] = int(a)
    elif o in ('-p', '--pool-size='):
      scraper_opts['pool_size'] = int(a)
    elif o in ('-a', '--user-agent='):
      scraper_opts['headers']['user-agent'] = a
    elif o in ('-c', '--cookie='):
      scraper_opts['headers']['cookie'] = a
    elif o in ('-m', '--save-media'):
      scraper_opts['save_media'] = True
    elif o in ('-s', '--save-attachments'):
      scraper_opts['save_attachments'] = True
    elif o in ('-u', '--save-users'):
      scraper_opts['save_users'] = True
    elif o in ('-o', '--output='):
      scraper_opts['output'] = a
    elif o in ('-l', '--log-file='):
      scraper_opts['log_file'] = a
    elif o == '-v':
      if scraper_opts['log_level'] == logging.WARNING:
        scraper_opts['log_level'] = logging.INFO
      elif scraper_opts['log_level'] == logging.INFO:
        scraper_opts['log_level'] = logging.DEBUG

  if scraper_opts['log_file'] is None:
    scraper_opts['log_level'] = logging.INFO

  if scraper_opts['output'] is None:
    scraper_opts['output'] = requests.utils.urlparse(scraper_opts['url']).netloc

  locale.setlocale(locale.LC_TIME, scraper_opts['lc_time'])

  global g_log_level
  global g_log_format
  global g_log_file

  g_log_file = scraper_opts['log_file']
  g_log_level = scraper_opts['log_level']


  logQueue = multiprocessing.Queue(-1)
  listener = multiprocessing.Process(target=listener_process, args=(logQueue, listener_configurer, g_log_level, g_log_format, g_log_file))
  listener.start()
  worker_configurer(logQueue, g_log_level)
  multiprocessing.current_process().name="Main"

  start_ = time.time()
  logging.info('==== Starting phpBB scraper for {}'.format(scraper_opts['url']) + ' ====')

  scraper = PhpBBScraper(scraper_opts, forums, topics)
  pool = mp.Pool(processes=scraper_opts['max_workers'], initializer=init_worker, initargs=(worker_configurer,logQueue,g_log_level))

  doneProcessing = False
  while doneProcessing == False:
    for pages in pool.imap_unordered(func=scrape_worker, iterable=scraper.scrape()):
        if pages is None:
          scraper.processed()
          continue
        else:
          scraper.enqueue(pages)
          scraper.processed()

    if scraper.is_done():
        doneProcessing = True  # Exit the loop if all work is done
    
  pool.close()
  pool.join()

  logging.debug('DONE: phpBB scraper for {}'.format(scraper_opts['url']))

  media = scraper.force_merge()
  if media:
    logging.info('FORCE: Fetching media from unmerged topics')
    pool = mp.Pool(processes=scraper_opts['max_workers'], initializer=init_worker, initargs=(worker_configurer,logQueue,g_log_level))
    scraper.enqueue(media)
    doneProcessing = False
    while doneProcessing == False:
      for pages in pool.imap_unordered(scrape_worker, scraper.scrape()):
        if pages is None:
          continue
        else:
          scraper.enqueue(pages)
          scraper.processed()

    if scraper.is_done():
        doneProcessing = True  # Exit the loop if all work is done

    pool.close()
    pool.join()

  scraper.stats()
  end_ = time.time()
  logging.info('==== Completed in {}'.format(datetime.timedelta(milliseconds=int((end_ - start_) * 1000))) + ' ====')

  logQueue.put_nowait(None)
  listener.join()

if __name__ == '__main__':
  main()
