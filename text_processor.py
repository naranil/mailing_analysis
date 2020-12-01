import regex as re
from emoji import UNICODE_EMOJI
from six import int2byte, unichr
from six.moves import html_entities

ENT_RE = re.compile(r'&(#?(x?))([^&;\s]+);')
HTML_RE = re.compile(r'<.*?>')

URLS_STRING = r"""(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))"""
URLS_STRING = r"((?<=[^a-zA-Z0-9])(?:https?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|sm){1}(?:\/[a-zA-Z0-9]{1,})*)"
PHONE_NUMBER_STRING = "[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[-\s\./0-9]*$"
MENTION_STRING = r"""\B\@[A-Za-z0-9_.](?:(?:[A-Za-z0-9_.]|(?:\\.(?!\\.))){0,28}(?:[A-Za-z0-9_.]))?"""
HASHTAG_STRING = r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
EMAIL_STRING = r"""[\w.+-]+@[\w-]+\.(?:[\w-]\.?)+[\w-]"""
# Remaining string : includes instagram username with this pattern : toto._.toto
ACCENTED_CHARACTERS = 'ÀàÁáÂâÃãÄäÇçÈèÉéÊêËëÌìÍíÎîÏïÑñÒòÓóÔôÕõÖöŠšÚùÛúÜûÙüÝýŸÿŽž'
REMAINING_STRING = r"""([A-Za-z0-9%s_](?:(?:[A-Za-z0-9%s_]|(?:\.(?!\.))){0,28}(?:[A-Za-z0-9%s_]))?)|(?:[^\W\d_](?:[^\W\d_]|['\-_])+[^\W\d_])|(?:[+\-]?\d+[,/.:-]\d+[+\-]?)|(?:[\w_]+)|(?:\.(?:\s*\.){1,})|(?:\S)""" % (ACCENTED_CHARACTERS, ACCENTED_CHARACTERS, ACCENTED_CHARACTERS)
PUNCTUATION = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•’@...”“'
MERGE_FIELD_STRING = r"""{{.*?}}"""
EMOJI_STRING = '[(' + '|'.join(list(UNICODE_EMOJI.keys())[:1035] + list(UNICODE_EMOJI.keys())[1037:]) + ')]+'

tokens_types = ['token', 'hashtag', 'mention', 'url', 'mail', 'merge_field', 'emoji']

REGEXP_STRINGS = {
    'hashtag': HASHTAG_STRING,
    'mention': MENTION_STRING,
    'url': URLS_STRING,
    'mail': EMAIL_STRING,
    'merge_field': MERGE_FIELD_STRING,
    'emoji': EMOJI_STRING,
    'token': REMAINING_STRING
}

HANG_REGEXP = re.compile(r'([^a-zA-Z0-9])\1{3,}')
WORD_REGEXP = re.compile(r"""(%s)""" % "|".join(["(?P<{}>{})".format(i, v) for i, v in REGEXP_STRINGS.items()]), re.VERBOSE | re.I | re.UNICODE)
SPECIFIC_URLS = ['instagram', 'facebook', 'tiktok', 'youtube', 'paypal', 'amazon', 'bit.ly', 'generator.com', 'calendly']

def _str_to_unicode(text, encoding=None, errors='strict'):
    if encoding is None:
        encoding = 'utf-8'
    if isinstance(text, bytes):
        return text.decode(encoding, errors)
    return text

def _replace_html_entities(text, keep=(), remove_illegal=True, encoding='utf-8'):
    """
    From nltk
    """

    def _convert_entity(match):
        entity_body = match.group(3)
        if match.group(1):
            try:
                if match.group(2):
                    number = int(entity_body, 16)
                else:
                    number = int(entity_body, 10)
                if 0x80 <= number <= 0x9F:
                    return int2byte(number).decode('cp1252')
            except ValueError:
                number = None
        else:
            if entity_body in keep:
                return match.group(0)
            else:
                number = html_entities.name2codepoint.get(entity_body)
        if number is not None:
            try:
                return unichr(number)
            except ValueError:
                pass

        return "" if remove_illegal else match.group(0)

    return ENT_RE.sub(_convert_entity, _str_to_unicode(text, encoding))

def _is_emoji(s):
    return s in UNICODE_EMOJI

class SocialMediaTokenizer():
    def __init__(self,
                 text,
                 remove_html=True,
                 lower=True,
                 detect_emoji=True,
                 detect_punctuation=True,
                 stopwords='remove',
                 merge_field='keep',
                 token='keep',
                 mention='replace',
                 hashtag='keep',
                 url='replace',
                 mail='replace',
                 punctuation='remove',
                 emoji='keep',
                 stopwords_list=None,
                 specify_url_type=False):
        self.lower = lower
        self.clean_text_ = str(text)
        if self.lower:
            self.clean_text_ = self.clean_text_.lower()
        self.clean_text_ = HTML_RE.sub('', self.clean_text_)
        self.remove_html = remove_html
        if self.remove_html:
            self.clean_text_ = _replace_html_entities(self.clean_text_)
        self.clean_text_ = HANG_REGEXP.sub(r'\1\1\1', self.clean_text_)
        self.detect_emoji = detect_emoji
        self.detect_punctuation = detect_punctuation
        self.token = token
        self.mention = mention
        self.hashtag = hashtag
        self.url = url
        self.mail = mail
        self.merge_field = merge_field
        self.punctuation = punctuation
        self.emoji = emoji
        self.stopwords = stopwords
        self.stopwords_list = stopwords_list
        self.specify_url_type = specify_url_type

    def tokenize(self):
        detect_punctuation = self.detect_punctuation
        detect_emoji = self.detect_emoji
        stopwords_list = self.stopwords_list
        specify_url_type = self.specify_url_type
        self.tokens_ = []
        for match in WORD_REGEXP.finditer(self.clean_text_):
            token_dict = match.groupdict()
            for token_type in tokens_types:
                if token_dict[token_type] is not None:
                    if detect_punctuation:
                        if token_dict[token_type] in PUNCTUATION:
                            self.tokens_.append((token_dict[token_type], 'punctuation'))
                            break
                    if stopwords_list is not None:
                        if token_dict[token_type] in stopwords_list:
                            self.tokens_.append((token_dict[token_type], 'stopword'))
                            break
                    if specify_url_type and token_type == 'url':
                        found = False
                        for url_type in SPECIFIC_URLS:
                            if url_type in token_dict[token_type]:
                                self.tokens_.append((token_dict[token_type], token_type + '_' + '_'.join(url_type.split('.'))))
                                found =True
                                break
                        if found:
                            break
                    self.tokens_.append((token_dict[token_type], token_type))
                    break

    def process_text(self):
        rules = {
            'token': self.token,
            'mention': self.mention,
            'hashtag': self.hashtag,
            'merge_field': self.merge_field,
            'url': self.url,
            'mail': self.mail,
            'punctuation': self.punctuation,
            'emoji': self.emoji,
            'stopword':self.stopwords
        }
        processed_tokens = []
        for (tk, tk_name) in self.tokens_:
            rule = rules[tk_name] if tk_name in rules else rules['url']
            if rule == 'remove':
                pass
            elif rule == 'keep':
                processed_tokens.append(tk)
            elif rule == 'replace':
                processed_tokens.append('[' + tk_name + ']')
            elif rule == 'strip':
                if tk_name in ['hashtag', 'mention']:
                    processed_tokens.append(tk[1:])
                else:
                    ValueError("Strip mode just for hashtags and mentions")
            else:
                ValueError("Rule should be either remove, keep, replace and strip")
        self.clean_text_ = ' '.join(processed_tokens)