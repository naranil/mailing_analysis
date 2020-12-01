import numpy as np
import textstat
from html_utils import MailObjectAnalyzer, MailBodyAnalyzer
from textblob import TextBlob
from collections import Counter
import textdistance
import re

readability_metrics_functions = {
    'flesch_reading_ease': textstat.flesch_reading_ease,
    'flesch_kincaid_grade': textstat.flesch_kincaid_grade,
    'coleman_liau_index': textstat.coleman_liau_index,
    'automated_readability_index': textstat.automated_readability_index,
    'dale_chall_readability_score': textstat.dale_chall_readability_score,
    'difficult_words': textstat.difficult_words,
    'linsear_write_formula': textstat.linsear_write_formula,
    'gunning_fog': textstat.gunning_fog
}

first_person_pronouns = ['i', 'me', 'my', 'mine', 'myself']
second_person_pronouns = ['you', 'your', 'yours', 'yourself', 'yourselves']
first_person_plural_pronouns = ['we', 'us', 'our', 'ours', 'ourselves']
third_person_plural_pronouns = ['they', 'them', 'their', 'theirs', 'themselves']
third_person_pronouns = ['he', 'she', 'him', 'her', 'his', 'hers', 'himself', 'herself']

def total_image_surface(images_infos):
    total_surface = 0
    for img_info in images_infos:
        height, width = img_info['height'], img_info['width']
        if height is not None and width is not None:
            total_surface += height*width
    return total_surface

def custom_field_output(x):
    fields = MailObjectAnalyzer(x, lower=True).extract_cutsom_fields()
    if len(fields) != 0:
        if fields[0] in ['{{influencer_name}}', '{{instagram_name}}']:
            return fields[0]
        else:
            return "{{other}}"
    else:
        return '{{None}}'
    
def readability_metrics(text_serie):
    results = text_serie.agg(list(readability_metrics_functions.values()))
    return results

def pronoun_metrics(text):
    results = []
    zen = TextBlob(text)
    words = zen.words
    counter_words = Counter(words)
    
    nb_i = 0
    nb_we = 0
    nb_we_and_i = 0
    nb_he = 0
    nb_they = 0
    nb_he_and_they = 0
    nb_you = 0
    
    for p in first_person_pronouns:
        if p in counter_words:
            nb_i += counter_words[p]
            nb_we_and_i += counter_words[p]
    
    for p in first_person_plural_pronouns:
        if p in counter_words:
            nb_we += counter_words[p]
            nb_we_and_i += counter_words[p]
            
    for p in third_person_pronouns:
        if p in counter_words:
            nb_he += counter_words[p]
            nb_we_and_i += counter_words[p]
            
    for p in second_person_pronouns:
        if p in counter_words:
            nb_you += counter_words[p]
            
    ratio_you_we = nb_you / nb_we if nb_we != 0 else nb_you
    ratio_you_i = nb_you / nb_i if nb_i != 0 else nb_you
    ratio_you_first = nb_you / nb_we_and_i if nb_we_and_i != 0 else nb_you
            
    return [nb_i, nb_we, nb_we_and_i, nb_he, nb_they, nb_he_and_they, nb_you, ratio_you_we, ratio_you_i, ratio_you_first]

def sentiment_analyzer(text, nb_first=3):
    blob = TextBlob(text)
    polarities = []
    subjectivities = []
    for sentence in blob.sentences[:nb_first]:
        polarities.append(sentence.polarity)
        subjectivities.append(sentence.subjectivity)
    while len(subjectivities) < nb_first:
        subjectivities.append(0)
    while len(polarities) < nb_first:
        polarities.append(0)
    max_polarity = max([sentence.polarity for sentence in blob.sentences])
    max_subjectivity = max([sentence.subjectivity for sentence in blob.sentences])
    
    return polarities + subjectivities + [max_polarity, max_subjectivity]

def company_occurences(raw_text_lower, company_name_lower, levenshtein_th=0.3):
    if company_name_lower is None:
        return 0, 0
    nb_occurences = len(re.findall(company_name_lower, raw_text_lower))
    # Make better processing for text
    levensthein_metrics = \
        np.array(
            [textdistance.levenshtein.normalized_similarity(company_name_lower, tk) for tk in raw_text_lower.split(' ')]
        )
    levensthein_occurence = sum(levensthein_metrics[levensthein_metrics >= levenshtein_th])
    return [nb_occurences, levensthein_occurence]

def extract_infos_from_html(df, user_id_to_company_id, company_id_to_name):
    df['raw_text'] = df['templated_body'].apply(
        lambda x: MailBodyAnalyzer(x, lower=False).get_raw_text()
    )
    df = df[df['raw_text'] != '']
    df.reset_index(inplace=True, drop=True)
    df['raw_text_lower'] = df['raw_text'].apply(lambda x: x.lower())
    df['clean_text'] = df['raw_text_lower'].apply(lambda x: MailBodyAnalyzer(x, lower=True).get_clean_text())
    df['nb_tokens_object'] = \
        df['templated_object'].apply(
            lambda x: len(MailObjectAnalyzer(x, lower=True).get_clean_tokens()))

    df['object_length'] = \
        df['templated_object'].apply(lambda x: MailObjectAnalyzer(x, lower=True).len_str())

    df['nb_custom_fields_object'] = \
        df['templated_object'].apply(
            lambda x: len(MailObjectAnalyzer(x, lower=True).extract_cutsom_fields()))
    df['has_fields_object'] = df['nb_custom_fields_object'] > 0

    df['custom_fields'] = df['templated_object'].apply(lambda x: custom_field_output(x))

    df['body_text'] = df['templated_body'].apply(lambda x: MailBodyAnalyzer(x, lower=True).get_clean_tokens())
    df['body_text'] = df['body_text'].apply(lambda x: ' '.join(x))

    df['nb_tokens_body'] = \
    df['templated_body'].apply(lambda x: len(MailBodyAnalyzer(x, lower=False).get_clean_tokens()))

    df['body_length'] = df['body_text'].apply(len)
    df['html_body_length'] = df['templated_body'].apply(len)
    df['nb_html_tags_body'] = df['templated_body'].apply(lambda x: MailBodyAnalyzer(x, lower=False).get_tags_number())

    df['body_custom_fields'] = \
    df['templated_body'].apply(lambda x: MailBodyAnalyzer(x, lower=False).extract_cutsom_fields())

    selected_fields = ['{{influencer_name}}', '{{first_name}}', '{{instagram_name}}', '{{largest_social_media_type}}',
                       '{{instagram_followers}}', '{{application_link}}', '{{largest_social_media_name}}', '{{price}}']

    for field in selected_fields:
        df['body_' + field[1:-1]] = df['body_custom_fields'].apply(lambda x: field in x)

    df['nb_custom_fields_body'] = df['body_custom_fields'].apply(lambda x: len(x))
    df['has_fields_body'] = df['nb_custom_fields_body'] > 0
    
    df['object_custom_fields'] = \
    df['templated_object'].apply(lambda x: MailObjectAnalyzer(x, lower=False).extract_cutsom_fields())

    selected_fields = ['{{influencer_name}}', '{{first_name}}', '{{instagram_name}}', '{{largest_social_media_type}}',
                       '{{instagram_followers}}', '{{application_link}}', '{{largest_social_media_name}}', '{{price}}']

    for field in selected_fields:
        df['object_' + field[1:-1]] = df['object_custom_fields'].apply(lambda x: field in x)

    df['nb_custom_fields_object'] = df['object_custom_fields'].apply(lambda x: len(x))
    df['has_fields_object'] = df['nb_custom_fields_object'] > 0

    df['images_infos'] = df['templated_body'].apply(lambda x:
                                                    MailBodyAnalyzer(x,
                                                                     lower=True,
                                                                     asyncio_loop=None,
                                                                     img_size_url_finder=False).get_images_infos())
    df['nb_images'] = df['images_infos'].apply(len)
    df['total_images_surface_norm'] = df['images_infos'].apply(total_image_surface).apply(np.sqrt)
    
    r_metrics_body = readability_metrics(df['raw_text'])
    for col in r_metrics_body.columns:
        df[f'{col}_body'] = r_metrics_body[col]

    r_metrics_object = readability_metrics(df['raw_text'])
    for col in r_metrics_object.columns:
        df[f'{col}_object'] = r_metrics_object[col]
        
    df['nb_sentences'] = df['raw_text'].apply(lambda x: len(TextBlob(x).sentences))
    
    pronoun_columns = ['nb_i', 'nb_we', 'nb_we_and_i', 'nb_he', 'nb_they', 'nb_he_and_they', 'nb_you', 'ratio_you_we', 'ratio_you_i', 'ratio_you_first']
    for col in pronoun_columns:
        df[col] = 0
    
    processed_data = df['raw_text_lower'].apply(lambda x: pronoun_metrics(x))
    processed_data = np.array([np.array(i) for i in processed_data.values])
    df[pronoun_columns] = processed_data
    
    df['currency'] = df['raw_text'].apply(lambda x: '$' in x or '£' in x or '€' in x)
    
    sentiment_data = np.array(list(df['raw_text'].apply(sentiment_analyzer).values))
    sentiment_features = ['polarity_1', 'polarity_2', 'polarity_3', 'subjectivity_1', 'subjectivity_2', 'subjectivity_3',
                          'max_polarity', 'max_subjectivity']
    for id_sentiment, sent in enumerate(sentiment_features):
        df[sent] = sentiment_data[:, id_sentiment]
        
    df['overall_polarity'] = df['raw_text'].apply(lambda x: TextBlob(x).sentiment.polarity)
    df['overall_subjectivity'] = df['raw_text'].apply(lambda x: TextBlob(x).sentiment.subjectivity)
        
    df['company_id'] = df['created_by'].apply(lambda x: user_id_to_company_id[x] if x in user_id_to_company_id else None)
    df['company_name'] = df['company_id'].apply(lambda x: company_id_to_name[x] if not np.isnan(x) else None)
    df['company_name_lower'] = df['company_name'].apply(lambda x: x.lower() if x is not None else None)
    company_occurences_data = np.array(
        list(
            df[['body_text', 'company_name_lower']].apply(lambda x: company_occurences(x[0], x[1]), axis=1).values
        )
    )
    df['nb_occurences'] = company_occurences_data[:, 0]
    df['levensthein_occurence'] = company_occurences_data[:, 1]
    
    df['influencer_company_ratio'] = (df['nb_we_and_i'] + df['nb_occurences']) / (df['nb_you'] + df['nb_custom_fields_body'])
    
    return df