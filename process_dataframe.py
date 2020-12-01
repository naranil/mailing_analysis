import re
import datetime
import logging
from collections import Counter
import pandas as pd

USEFUL_COLUMNS_THREADS = ['id', 'influencer_id', 'influencer_email', 'user_email', 'is_mailing', 'holder_id', 'replied']
USEFUL_COLUMNS_MAILINGS = ['id', 'name', 'active', 'owned_by', 'created_by', 'provider_type']
INBOX_DEMO_MAILS = set([
    'doug.rosencrans@upfluence.com', 'jack.blattman@upfluence.com', 'themba.daniels@upfluence.com', 'soufiane.elhamidi@upfluence.com',\
    'natanyel.kazoula@upfluence.com', 'nicole.wilson@upfluence.com', 'kallan.smith@upfluence.com', 'meidi.rauxsong@upfluence.com', \
    'paul.kahn@upfluence.com', 'connor.golden@upfluence.com', 'santiago.alzate@upfluence.com', 'kevin.creusy@upfluence.com', \
    'thomas.jones@upfluence.com', 'stephanie.laure@upfluence.com', 'tom.boster@upfluence.com', 'ana.kramer@upfluence.com', \
    'danu.hacker@upfluence.com', 'natanyel.kazoula@upfluence.com', 'rosangela.guerra@upfluence.com', 'amandine.henrard@upfluence.com', \
    'siobhan.donovan@upfluence.com', 'celena.danahy@upfluence.com', 'siobhan.donovan+1@upfluence.com', 'mindy.soh@upfluence.com', \
    'simon.boxus@upfluence.com', 'soufiane.elhamidi+12@upfluence.com', 'dimitri.thadal@upfluence.com', 'pierre.bonhomme@upfluence.com',
    'philippe.ndiaye@upfluence.com', 'hilina.bizuwork@upfluence.com', 'an.nguyen@upfluence.com', 'tiffany.shek@upfluence.com', \
    'arielle.williams@upfluence.com', 'madeline.poe@upfluence.com', 'viresh.jain@upfluence.com', 'yadira.ruiz@upfluence.com', \
    'nicole.feldman@upfluence.com', 'mohamed.hegazi@upfluence.com', 'natanyel@upfluence.com', 'jake.mandelkorn@upfluence.com', \
    'morgane.lebras@upfluence.com', 'tianyi.liu@upfluence.com', 'yann.chaboute@upfluence.com', 'simon.boxus+2@upfluence.com', \
    'robert.johnson@upfluence.com', 'konstancija.lesyte@upfluence.com', 'thomas.gallice@upfluence.com', 'antonin.durand@upfluence.com',\
    'thomas.isaac@upfluence.com', 'jacob.wesdorp@upfluence.com', 'vivien.yang@upfluence.com', 'alice.cichon@upfluence.com', \
    'jessica.gales@upfluence.com', 'adam.shapiro@upfluence.com'
])
MIN_RESPONSE_TIME = 30
MIN_RESPONSE_TIME_NON_RE = 600
RESPONSE_PATTERNS = ['re:', '回复:', '回覆:', 'sv:', 'antw:', 'vs:', 'ref:', 'aw:', 'ΑΠ:', 'bls:', 'res:', 'odp:', 'ynt:']
RESPONSE_PATTERNS += ['fw:', '轉寄:', '轉寄:', 'vs:', 'doorst:', 'vl:', 'tr:', 'wg:', 'ΠΡΘ:', 'trs:', 'vb:', 'rv:', 'enc:', 'pd:', 'İLT']
RESPONSE_PATTERNS += [r[:-1] + ' :' for r in RESPONSE_PATTERNS]
MERGE_FIELDS = ['{{influencer_name}}', '{{first_name}}', '{{instagram_name}}', '{{largest_social_media_type}}',
                '{{instagram_followers}}', '{{application_link}}', '{{largest_social_media_name}}', '{{price}}']

def load_mailings_dataframe(mailings_path,
                            start_datetime,
                            end_datetime):
    mailings_df = pd.read_csv(mailings_path, sep='\t')
    mailings_df['created_at'] = mailings_df['created_at'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    mailings_df['updated_at'] = mailings_df['updated_at'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    mailings_df = mailings_df[
        (mailings_df['created_at'] >= start_datetime) & (mailings_df['created_at'] <= end_datetime)]
    mailings_df.reset_index(inplace=True, drop=True)

    logging.info(f"Number of mailings campaigns : {mailings_df.shape[0]}")

    return mailings_df

def load_threads_dataframe(threads_path,
                           start_datetime,
                           end_datetime,
                           mailings_ids):
    threads_df = pd.read_csv(threads_path, sep='\t')
    if 'Unnamed: 0' in threads_df.columns:
        threads_df.drop(['Unnamed: 0'], axis=1, inplace=True)
    threads_df = threads_df[threads_df['holder_id'].isin(mailings_ids)]
    threads_df['created_at'] = threads_df['created_at'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    threads_df['updated_at'] = threads_df['updated_at'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    threads_df = threads_df[(threads_df['created_at'] >= start_datetime) & (threads_df['created_at'] <= end_datetime)]
    threads_df['influencer_email'] = threads_df['influencer_email'].apply(lambda x: x.lower())
    threads_df['user_email'] = threads_df['user_email'].apply(lambda x: x.lower())
    threads_df['is_mailing'] = threads_df['holder_type'] == 'Inbox::Model::Mailing'
    threads_df.reset_index(inplace=True, drop=True)

    logging.info(f"Number of threads : {threads_df.shape[0]}")

    return threads_df

def load_emails_inbox_dataframe(emails_inbox_path,
                                start_datetime,
                                end_datetime,
                                threads_ids):
    emails_inbox_df = pd.read_csv(emails_inbox_path, sep='\t')
    emails_inbox_df = emails_inbox_df[emails_inbox_df['thread_id'].isin(threads_ids)]
    emails_inbox_df['created_at'] = emails_inbox_df['created_at'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    emails_inbox_df['updated_at'] = emails_inbox_df['updated_at'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    emails_inbox_df = emails_inbox_df[
        (emails_inbox_df['created_at'] >= start_datetime) & (emails_inbox_df['created_at'] <= end_datetime)]
    emails_inbox_df.rename(columns={'object': 'mail_object'}, inplace=True)
    emails_inbox_df['mail_object'] = emails_inbox_df['mail_object'].fillna('')
    emails_inbox_df.reset_index(inplace=True, drop=True)

    logging.info(f"Number of emails : {emails_inbox_df.shape[0]}")

    return emails_inbox_df

def merge_dataframes(emails_inbox_df,
                     threads_df,
                     mailings_df):
    df = pd.merge(emails_inbox_df, threads_df[USEFUL_COLUMNS_THREADS], how='left', left_on='thread_id', right_on='id',
                  suffixes=('', '_y'))
    del df['id_y']
    df = pd.merge(df, mailings_df[USEFUL_COLUMNS_MAILINGS], how='left', left_on='holder_id', right_on='id',
                  suffixes=('', '_y'))
    del df['id_y']

    return df

def clean_merged_emails_dataframe(df):
    df = df.sort_values(by=['holder_id', 'thread_id', 'created_at'])
    df.reset_index(inplace=True, drop=True)

    unique_mails_threads_ids = set([i for (i, ct) in Counter(df['thread_id']).items() if ct == 1])
    df['single_mail'] = df['thread_id'].isin(unique_mails_threads_ids)
    df['answered_mail'] = (1 - df['single_mail']).astype(bool)
    df['is_influencer_reply'] = df['response'] == True

    # Abnormal time response
    previous_thread = -1
    previous_time = -1
    time_diff = []

    for thread_id, created_at in df[['thread_id', 'created_at']].values:
        if thread_id != previous_thread:
            time_diff.append(created_at)
            previous_thread = thread_id
        else:
            time_diff.append(previous_time)
        previous_time = created_at

    previous_thread = -1
    previous_reply_type = -1
    is_reply = []

    for thread_id, is_inf_reply in df[['thread_id', 'is_influencer_reply']].values:
        if thread_id != previous_thread:
            is_reply.append(False)
            previous_thread = thread_id
        else:
            if previous_reply_type != is_inf_reply:
                is_reply.append(True)
            else:
                is_reply.append(False)
        previous_reply_type = is_inf_reply

    df['is_reply'] = is_reply
    df['time_difference'] = df['created_at'] - pd.Series(time_diff)
    df['timestamp_difference'] = df['created_at'].apply(lambda x: x.timestamp()) - \
                                 pd.Series(time_diff).apply(lambda x: x.timestamp())



    df = df[~df['user_email'].isin(INBOX_DEMO_MAILS)]
    df.reset_index(inplace=True, drop=True)

    potential_automatic_email_ids = \
    df[(df['is_influencer_reply']) &
       ((df['timestamp_difference'] <= MIN_RESPONSE_TIME) |
       ~(df['mail_object'].str.contains('|'.join(RESPONSE_PATTERNS), flags=re.IGNORECASE, regex=True) == True))].index

    logging.info(f"Number of potential automatic responses : {len(potential_automatic_email_ids)}")
    logging.info(f"% of potential automatic responses :\ "
                 f"{100*len(potential_automatic_email_ids) / df[df['is_influencer_reply']].shape[0]:.2f} %")

    tmp = df[(df['is_influencer_reply']) &
             ~(df['mail_object'].str.contains('|'.join(RESPONSE_PATTERNS), flags=re.IGNORECASE, regex=True) == True)]

    logging.info(f"% reponses without re and fwd : {100*len(tmp) / df[df['is_influencer_reply']].shape[0]:.2f}%")

    df.drop(index=potential_automatic_email_ids, inplace=True)
    df.reset_index(inplace=True, drop=True)

    logging.info(f"Remaining rows : {len(df)}")

    return df

def keep_only_first_mail_and_response(cleaned_df):
    authorized_mails_ids = []
    first_reply_found = False

    previous_thread = -1

    for id, thread_id, is_influencer_reply in cleaned_df[['id', 'thread_id', 'is_influencer_reply']].values:
        if thread_id != previous_thread:
            authorized_mails_ids.append(id)
            previous_thread = thread_id
            first_reply_found = False
        else:
            if is_influencer_reply and not first_reply_found:
                authorized_mails_ids.append(id)
                first_reply_found = True

    authorized_mails_ids = set(authorized_mails_ids)
    cleaned_df = cleaned_df[cleaned_df['id'].isin(authorized_mails_ids)]
    cleaned_df.reset_index(inplace=True, drop=True)

    return cleaned_df

def keep_mailings_min_threads(cleaned_df, nb_min_threads, mailings_df=None):
    thread_count = cleaned_df[~cleaned_df['is_reply']].groupby('holder_id')['thread_id'].count()
    authorized_holder_ids = list(thread_count[thread_count >= nb_min_threads].index)

    cleaned_df = cleaned_df[cleaned_df['holder_id'].isin(authorized_holder_ids)]
    cleaned_df.reset_index(inplace=True, drop=True)

    if mailings_df is not None:
        remaining_holder_ids = cleaned_df['holder_id'].unique()
        average_answered_mails = cleaned_df[~cleaned_df['is_influencer_reply']].groupby('holder_id')\
        ['answered_mail'].mean().sort_values()
        nb_answered_mails = cleaned_df[~cleaned_df['is_influencer_reply']].groupby('holder_id')\
        ['answered_mail'].sum().sort_values()
        mailings_df = \
            pd.merge(
                mailings_df[mailings_df['id'].isin(remaining_holder_ids)],
                pd.DataFrame({'id': average_answered_mails.index, 'response_rate': average_answered_mails.values}))
        mailings_df = \
            pd.merge(
                mailings_df[mailings_df['id'].isin(remaining_holder_ids)],
                pd.DataFrame({'id': nb_answered_mails.index, 'nb_answered_mails': nb_answered_mails.values}))
        thread_count = thread_count.reset_index()
        thread_count.columns = ['holder_id', 'nb_threads']
        mailings_df = pd.merge(mailings_df, thread_count, left_on='id', right_on='holder_id')

        return cleaned_df, mailings_df

    return cleaned_df, None
