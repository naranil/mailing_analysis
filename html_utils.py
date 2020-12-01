import re
from bs4 import BeautifulSoup
from fastimage.fastimage.detect import get_size
import asyncio
from text_processor import SocialMediaTokenizer

class MailObjectAnalyzer(object):
    def __init__(self,
                 mail_object,
                 lower=False):
        if type(mail_object) == str:
            self.mail_object = mail_object.lower() if lower else mail_object
        else:
            self.mail_object = ''

    def __len__(self):
        return len(self.get_tokens())

    def __str__(self):
        return self.mail_object

    def len_str(self):
        return len(self.mail_object)

    def get_tokens(self):
        return [tk for tk in self.mail_object.split(' ') if tk != '']

    def get_clean_tokens(self):
        return re.findall("[a-zA-Z0-9]+|{{.*?}}", self.mail_object)

    def extract_cutsom_fields(self):
        return re.findall("{{.*?}}", self.mail_object)

class MailBodyAnalyzer(object):
    def __init__(self,
                 mail_body,
                 lower=True,
                 asyncio_loop=None,
                 img_size_url_finder=False):
        self.soup = BeautifulSoup(mail_body, features="html.parser")
        tags_to_remove = ['script', 'style']
        for tag in self.soup.find_all(tags_to_remove):
            tag.extract()
        self.lower = lower
        if type(mail_body) != str:
            self.mail_body = ''
        else:
            self.mail_body = mail_body
        self.raw_text_ = None
        self.asyncio_loop = asyncio_loop
        self.img_size_url_finder = img_size_url_finder

    def __str__(self):
        return self.mail_body

    def __len__(self):
        raw_text = self.get_raw_text()
        return len(raw_text)

    def get_raw_text(self):
        if self.raw_text_ is None:
            self.raw_text_ = re.sub(' +', ' ', self.soup.get_text(' ').replace(u'\xa0', u' '))
        if self.lower:
            self.raw_text_ = self.raw_text_.lower()
        return self.raw_text_
    
    def get_clean_text(self):
        raw_text = self.get_raw_text()
        smtk = SocialMediaTokenizer(raw_text, specify_url_type=True)
        smtk.tokenize()
        smtk.process_text()    
        return smtk.clean_text_

    def get_clean_tokens(self):
        raw_text = self.get_raw_text()
        return re.findall("[a-zA-Z0-9]+|{{.*?}}", raw_text)

    def extract_cutsom_fields(self):
        raw_text = self.get_raw_text()
        return re.findall("{{.*?}}", raw_text)
    
    def extract_specific_text_patterns(self):
        raw_text = self.get_raw_text()
        return re.findall("[.*?]", raw_text)

    def get_tags_number(self):
        return len(self.soup.find_all())

    def get_images_infos(self):
        images_tags = self.soup.findAll('img')
        if self.asyncio_loop is None:
            self.asyncio_loop = asyncio.get_event_loop()
        loop = self.asyncio_loop
        if len(images_tags) == 0:
            return []
        else:
            result = []
            for img in images_tags:
                src = img.get('src')
                r = {'height': None, 'width': None}
                if src is not None:
                    if (img.get('height') is None or img.get('width') is None) and self.img_size_url_finder:
                        width, height = loop.run_until_complete(get_size(src))
                    else:
                        width, height = img.get('width'), img.get('height')
                    if width is not None and height is not None:
                        #if width.endswith('px'):
                        #    width = width[:-2]
                        #if width.endswith('px;'):
                        #    width = width[:-3]
                        #if height.endswith('px'):
                        #    height = height[:-2]
                        #if height.endswith('px;'):
                        #    height = height[:-3]
                        height, width = re.findall('[0-9]+', height)[0], re.findall('[0-9]+', width)[0]
                        r = {'width': float(width), 'height': float(height)}
                result.append(r)
        return result