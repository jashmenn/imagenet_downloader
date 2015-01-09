#!/usr/bin/env python
# Copyright (c) 2014 Seiya Tokui
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import argparse
import imghdr
import Queue
import os
import socket
import sys
import tempfile
import threading
import time
import urllib2
import eventlet

def download(url, timeoutF, retry, sleep, verbose=False):
    """Downloads a file at given URL."""
    count = 0

    while True:
        try:
            if verbose:
                sys.stderr.write('Downloading: ' + url + ' ('+ str(count) +')\n')
            f = urllib2.urlopen(url, timeout=timeoutF)
            if f is None:
                raise Exception('Cannot open URL {0}'.format(url))
            content = f.read()
            f.close()
            break
        except urllib2.HTTPError as e:
            if 500 <= e.code < 600:
                count += 1
                if count > retry:
                    raise
            else:
                raise
        except urllib2.URLError as e:
            if isinstance(e.reason, socket.gaierror):
                count += 1
                time.sleep(sleep)
                if count > retry:
                    raise
            else:
                raise
        except:
            count += 1
            time.sleep(sleep)
            if count > retry:
                raise

    return content

def imgtype2ext(typ):
    """Converts an image type given by imghdr.what() to a file extension."""
    if typ == 'jpeg':
        return 'jpg'
    if typ is None:
        raise Exception('Cannot detect image type')
    return typ

def make_directory(path):
    if not os.path.isdir(path):
        os.makedirs(path)



def download_imagenet(list_filename,
                      out_dir,
                      timeoutF=10,
                      retry=10,
                      num_jobs=1,
                      sleep_after_dl=1,
                      verbose=False):
    """Downloads to out_dir all images whose names and URLs are written in file
    of name list_filename.
    """

    make_directory(out_dir)

    count_total = 0
    with open(list_filename) as list_in:
        for line in list_in:
            count_total += 1

    sys.stderr.write('Total: {0}\n'.format(count_total))

    num_jobs = max(num_jobs, 1)

    entries = []

    with open(list_filename) as list_in:
        for line in list_in:
            pair = line.strip().split(None, 1)
            if(len(pair) < 2):
              continue
            name, url = pair
            entries.append((name, url))

    counts_fail = [0 for i in xrange(num_jobs)]
    counts_success = [0 for i in xrange(num_jobs)]

    def consumer(entry):
      timeout = eventlet.timeout.Timeout(timeoutF)
      try:
          name, url = entry[0], entry[1]
      except:
          return

      try:
          if name is None:
              if verbose:
                  sys.stderr.write('Error: Invalid line: ' + line)
              counts_fail[i] += 1
              return

          # hacky way to cache files, will only cache jpgs
          directory = os.path.join(out_dir, name.split('_')[0])
          ext = "jpg" # hack
          path = os.path.join(directory, '{0}.{1}'.format(name, ext))
          if os.path.isfile(path):
            if verbose:
                sys.stderr.write('Already have: ' + path + ' . Skipping\n')
            counts_success[i] += 1 # track skips?
            return

          content = download(url, timeoutF, retry, sleep_after_dl, verbose)
          ext = imgtype2ext(imghdr.what('', content))
          path = os.path.join(directory, '{0}.{1}'.format(name, ext))
          try:
              make_directory(directory)
          except:
              pass
          with open(path, 'w') as f:
              f.write(content)
          counts_success[i] += 1
          timeout.cancel()
          time.sleep(sleep_after_dl)

      except Exception as e:
          timeout.cancel()
          counts_fail[i] += 1
          if verbose:
              sys.stderr.write('Error: {0}: {1}\n'.format(name, e))

    def message_loop():
        if verbose:
            delim = '\n'
        else:
            delim = '\r'

        while True:
            count_success = sum(counts_success)
            count = count_success + sum(counts_fail)
            rate_done = count * 100.0 / count_total
            if count == 0:
                rate_success = 0
            else:
                rate_success = count_success * 100.0 / count
            sys.stderr.write(
                '{0} / {1} ({2}%) done, {3} / {0} ({4}%) succeeded                    {5}'.format(
                    count, count_total, rate_done, count_success, rate_success, delim))

            time.sleep(1)
        sys.stderr.write('done')

    message_thread = threading.Thread(target=message_loop)

    message_thread.start()

    pool = eventlet.GreenPool(num_jobs)
    for body in pool.imap(consumer, entries):
      True

    message_thread.join()

    sys.stderr.write('\ndone\n')

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('list', help='Imagenet list file')
    p.add_argument('outdir', help='Output directory')
    p.add_argument('--jobs', '-j', type=int, default=1,
                   help='Number of parallel threads to download')
    p.add_argument('--timeout', '-t', type=int, default=10,
                   help='Timeout per image in seconds')
    p.add_argument('--retry', '-r', type=int, default=2,
                   help='Max count of retry for each image')
    p.add_argument('--sleep', '-s', type=float, default=1,
                   help='Sleep after download each image in second')
    p.add_argument('--verbose', '-v', action='store_true',
                   help='Enable verbose messages')
    args = p.parse_args()

    download_imagenet(args.list, args.outdir,
                      timeoutF=args.timeout, retry=args.retry,
                      num_jobs=args.jobs, verbose=args.verbose)
