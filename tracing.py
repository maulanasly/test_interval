from __future__ import division

import os
import re
import json
import pytz
import logging
import psycopg2
import xmltodict
import petl as etl

import intervals as Interval
from time import sleep
import sqlalchemy as sq
from datetime import datetime
from collections import OrderedDict
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

Base = declarative_base(
    metadata=MetaData(
        schema='warehouse'
    )
)


class XMLHelper(object):

    @classmethod
    def xml_to_json(cls, xml_value):
        if not xml_value:
            return None

        try:
            dict_value = xmltodict.parse(xml_value)
        except Exception as err:
            logging.debug(err)
            return None

        return cls.json_key_cleaner(dict_value['message'])

    @classmethod
    def json_key_cleaner(self, value):
        json_key = {}
        for key, val in value.iteritems():
            if isinstance(val, dict):
                val = self.json_key_cleaner(val)
            json_key[key.strip('@') if key.startswith('@') else key] = val
        return json_key


class dBConnection(object):

    @classmethod
    def connect(cls, host, user, password, dbname, db_schema):
        config = {
            'host': host,
            'user': user,
            'password': password,
            'dbname': dbname,
            'options': '-c search_path=%s' % db_schema
        }
        return psycopg2.connect(**config)


class Model():
    class BaseModel(object):
        __table_args__ = {
            'useexisting': True
        }

    class Rooms(Base, BaseModel):
        __tablename__ = 'rooms'
        room_id = sq.Column(sq.String(32), primary_key=True)
        create_at = sq.Column(sq.TIMESTAMP)
        end_time = sq.Column(sq.TIMESTAMP)
        creator = sq.Column(UUID())

    class Participations(Base, BaseModel):
        __tablename__ = 'participations'
        p_id = sq.Column(sq.BigInteger, primary_key=True, autoincrement=True)
        room_id = sq.Column(sq.String(32), sq.ForeignKey("rooms.room_id"))
        participant_id = sq.Column(UUID(), nullable=False)
        start_time = sq.Column(sq.TIMESTAMP, nullable=False)
        end_time = sq.Column(sq.TIMESTAMP, nullable=False)
        role = sq.Column(sq.String(32))
        init_time = sq.Column(sq.TIMESTAMP)
        reason_for_leaving = sq.Column(sq.String(32))

    @classmethod
    def connection(cls, config):
        destination_db_engine = create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(
                config['user'],
                config['password'],
                config['host'],
                '5432',
                config['dbname'],
            ),
            connect_args={'connect_timeout': 3600}
        )
        Base.metadata.create_all(destination_db_engine)


class Loader(object):

    def truncate_table(self, conn, tablenames=['participations', 'rooms']):
        cursor = conn.cursor()
        for tablename in tablenames:
            cursor.execute('TRUNCATE %s CASCADE' % tablename)
        conn.commit()
        conn.close()

    def store_to_db(self, conn, tablename, data):
        try:
            if etl.nrows(data) == 0:
                return None
        except TypeError:
            return None

        cursor = conn.cursor()
        sql = "INSERT INTO %s (%s) " % (tablename, ','.join(etl.header(data))) + "VALUES %s"
        execute_values(cursor, sql, etl.data(data))
        conn.commit()
        conn.close()


def load_file(file_name):
    with open(file_name) as f:
        fs = f.read()
        return json.loads(fs)


def id_str_to_int(id_str):
    return int(id_str.split('@')[0]) if id_str.split('@')[0].isdigit() else None


def modified_participants(participants):
    participants_temp = participants
    for participant, summary in participants.iteritems():
        participants_temp[participant].pop('profilePicture')
        participants_temp[participant].pop('fullName')
    return participants_temp


def get_summary_creation(summaries):
    return {
        'end_time': max(
            [max(interval) for _, summary in summaries.iteritems() for interval in summary['interval'] if 'interval' in summary]
        )
    }


def grouping_summary_by_room_id(summaries):
    final_summaries = {}
    for user_id, user_summary in group_summary_per_user(summaries).iteritems():
        final_summaries[user_id] = {
            'interval': format_interval(
                merge_summaries(user_summary, lower='joinedTime', upper='leaveTime')
            ),
            'role': user_summary[0]['role'],
        }
    return final_summaries


def get_initiated_time_interval(summaries):
    final_summaries = {}
    for user_id, user_summary in group_summary_per_user(summaries).iteritems():
        final_summaries[user_id] = {
            'interval': format_interval(
                merge_summaries(user_summary, lower='initiatedTime', upper='leaveTime')
            )
        }
    return final_summaries


def find_creator_and_create_at(summaries):
    initiated_time_list = {}
    for summary in summaries:
        for user_id, participant_summary in summary.iteritems():
            initiated_time = participant_summary.get('initiatedTime', 0)
            if initiated_time == 0:
                continue
            initiated_time_list[initiated_time] = user_id
    index = min(initiated_time_list)
    return {
        'creator': initiated_time_list[index],
        'create_at': date_to_timestr(index)
    }


def date_to_timestr(timestamp):
    return datetime.fromtimestamp(
        timestamp / 1000, tz=pytz.utc
    ).strftime('%Y-%m-%dT%H:%M:%S.%f')


def replace_calltime(participant_summary, timestamp):
    # TODO:
    # - Check the timezone information in all time attribute
    if 'joinedTime' not in participant_summary:
        return {}

    if 'leaveTime' not in participant_summary:
        participant_summary['leaveTime'] = timestamp
    return participant_summary


def group_summary_per_user(summaries):
    summaries_per_user = {}
    for (summary, timestamp) in summaries:
        for user_id, participant_summary in summary.iteritems():
            updated_participant_summary = replace_calltime(participant_summary, timestamp)
            if not updated_participant_summary:
                continue

            if user_id not in summaries_per_user:
                summaries_per_user[user_id] = [updated_participant_summary]
            else:
                summaries_per_user[user_id].append(updated_participant_summary)
    return summaries_per_user


def merge_summaries(user_summary, lower, upper):
    final_summary = Interval.empty()
    for interval in [Interval.closed(summary[lower], summary[upper]) for summary in user_summary]:
        final_summary |= interval
    return final_summary


def format_interval(user_interval):
    return [
        (date_to_timestr(interval.lower), date_to_timestr(interval.upper))
        for interval in user_interval
    ]


def storing_data_preparation(data):
    participations_data = etl.cut(
        data,
        'room_id',
        'summary'
    )
    participations = []
    participations_data = etl.data(participations_data)
    for (room_id, summary) in participations_data:
        for participant_id, summary_items in summary.iteritems():
            for interval in summary_items['interval']:
                start_time, end_time = interval
                participations.append(
                    {
                        'room_id': room_id,
                        'participant_id': participant_id,
                        'start_time': start_time,
                        'end_time': end_time,
                        'role': summary_items['role']
                    }
                )
    rooms = etl.cut(
        data,
        'room_id',
        'create_at',
        'end_time',
        'creator'
    )
    participations = etl.fromdicts(
        participations,
        header=[
            'room_id',
            'participant_id',
            'start_time',
            'end_time',
            'role',
            # 'init_time',
            # 'eason_for_leaving'
        ]
    )
    return (rooms, participations)


if __name__ == "__main__":
    config = {
        'host': '192.168.0.48',
        'user': 'data_warehouse',
        'password': None,
        'dbname': 'data_warehouse',
        'db_schema': 'warehouse'
    }
    Model().connection(config)
    number_of_record = 0
    loader = Loader()
    while True:
        data_loaded = load_file('./group_call.json')
        if len(data_loaded) <= number_of_record:
            logging.info("Continue to the next iteration ..")
            sleep(10)
            continue

        number_of_record = len(data_loaded)
        data = etl.fromdicts(data_loaded)
        converted_data = etl.convert(
            data,
            'xml',
            lambda r: XMLHelper.xml_to_json(r)
        )
        converted_data = etl.convert(
            converted_data,
            'txt',
            lambda r: json.loads(r.strip('group_call_summary#'))
        )
        converted_data = etl.select(
            converted_data,
            lambda r: len(re.split(r'\/', r['peer'])) == 1
        )
        converted_data = etl.addfield(converted_data, 'room_id', lambda r: r['txt'].get('roomId'))
        converted_data = etl.addfield(converted_data, 'from_id', lambda r: id_str_to_int(r['xml']['from']))
        converted_data = etl.addfield(converted_data, 'to_id', lambda r: id_str_to_int(r['xml']['to']))
        converted_data = etl.addfield(converted_data, 'message_id', lambda r: r['xml']['id'])
        converted_data = etl.addfield(converted_data, 'termination_reason', lambda r: r['txt']['reasonOfTermination'])
        converted_data = etl.addfield(converted_data, 'call_type', lambda r: r['txt']['callType'])
        converted_data = etl.addfield(converted_data, 'participants', lambda r: modified_participants(r['txt']['participants']))
        converted_data = etl.addfield(converted_data, 'timestamp_ms', lambda r: r['timestamp'] / 1000)

        aggregations = OrderedDict()
        aggregations['summary'] = ('participants', 'timestamp_ms'), grouping_summary_by_room_id
        aggregations['initiated_time'] = ('participants', 'timestamp_ms'), get_initiated_time_interval
        aggregations['creator_data'] = ('participants'), find_creator_and_create_at
        aggregated_summary = etl.aggregate(
            converted_data,
            key=('room_id'),
            aggregation=aggregations
        )

        external_ids = etl.fromdicts(
            [
                {
                    'id': '3979',
                    'external_id': '95109151-af77-11e9-94fa-a860b6030e49'
                },
                {
                    'id': '3980',
                    'external_id': '95d8c92e-af77-11e9-99b7-a860b6030e49'
                },
                {
                    'id': '3982',
                    'external_id': '97163c4a-af77-11e9-bdf9-a860b6030e49'
                }
            ],
            header=['id', 'external_id']
        )
        aggregated_summary = etl.addfield(aggregated_summary, 'call_info', lambda r: get_summary_creation(r['summary']))
        aggregated_summary = etl.unpackdict(aggregated_summary, 'call_info')
        aggregated_summary = etl.unpackdict(aggregated_summary, 'creator_data')

        file_name = 'datasets-%s.csv' % datetime.now().strftime('%Y%m%d%H%M%S')
        directory = 'csv'
        if not os.path.exists(directory):
            os.makedirs(directory)
        # etl.tocsv(aggregated_summary, './%s/%s' % (directory, file_name))
        # logging.info('This %s has been exported' % file_name)

        rooms, participations = storing_data_preparation(aggregated_summary)

        participations = etl.leftjoin(participations, external_ids, lkey='participant_id', rkey='id', rprefix='r_')
        participations = etl.cutout(
            participations,
            'participant_id'
        )
        participations = etl.rename(participations, 'r_external_id', 'participant_id')

        rooms = etl.leftjoin(rooms, external_ids, lkey='creator', rkey='id', rprefix='r_')
        rooms = etl.cutout(
            rooms,
            'creator'
        )
        rooms = etl.rename(rooms, 'r_external_id', 'creator')

        logging.info('Storing data %s to database' % file_name)
        loader.truncate_table(dBConnection.connect(**config))
        loader.store_to_db(dBConnection.connect(**config), tablename='rooms', data=rooms)
        loader.store_to_db(dBConnection.connect(**config), tablename='participations', data=participations)
        logging.info('This %s has been stored' % file_name)
        # TODO:
        # For each roomId and participants get all the interval associated with them
        #   - Loop for each summary, start with empty summary
        #   - Filling leave_time with timestamp_str if not available and there is joinTime in it
        #   - Create user_id field every time it's found one in the summary and
        #   - Merge the interval that are found using the interval library
        #   - Keep an array of the initTime per user
        # Make a csv with the interval per participants (It will have column of full summary, therapistA, therapistB, and patient)
        # Show this in readable time and skip days, month and years
        # Make other column for initTime
