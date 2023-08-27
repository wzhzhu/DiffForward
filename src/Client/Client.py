#!/usr/bin/python2
#coding:utf-8
import time
import gevent
from gevent.event import Event
import logging
import random
import collections
import math
import pandas as pd
import ForwardingClient
from ..Utils import LoadRecorder
import sys
import os
import LightweightIndex


class Client:
    '''**********************************************************************************************
        init
    **********************************************************************************************'''
    def __init__(self, volume_id, segment_id, log_path, busy_flag):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        self.volume_id = volume_id
        self.segment_id = segment_id
        self.busy_flag = busy_flag
        self.applier_log_path = log_path+str(volume_id)+'_applier.csv'
        self.path_log_path = log_path + str(volume_id)+ '_path.csv'
        self.applier_queue = collections.deque()
        self.path_queue = collections.deque()
        self.start_time = time.time()*1000000
        self.peak_dict = dict()
        self.peak_queue_dict = dict()
        self.peak_id = 0
        self.peak_size = 0 
        self.normal_speed = 0
        self.peak_traffic = 0
        self.first_peak_flag = 0
        self.normal_sync_time = 0
        self.peak_sync_time = 0
        self.index = LightweightIndex.LightweightIndex()

        self.server_score = dict()
        self.srate = dict()
        self.rrate = dict()
        self.token_num = dict()
        self.max_token_num = 10000
        self.last_arrival_time = dict()
        self.outstanding_requests_num = dict()
        self.queue_size = dict()
        self.process_rate = dict()

    def bind_req_server(self, ip, port, manager_ip, manager_port, busy_flag):
        self.request_processor = ForwardingClient.ForwardingClient(self.volume_id,self.segment_id, ip, port, manager_ip, manager_port, busy_flag)
        self.request_processor.index = self.index
        return

    def bind_buffer_server(self, bf):
        self.buffer_processor = bf
        return 

    def set_load_recorder(self, manager_ip, manager_port, report_gap):
        self.load_recorder = LoadRecorder.LoadRecorder(self.volume_id, self.segment_id, manager_ip, manager_port, report_gap)
        return

    def set_req_event_queue(self, write_event_queue, read_event_queue):
        self.write_event_queue = write_event_queue
        self.read_event_queue = read_event_queue
        self.request_processor.set_req_event_queue(write_event_queue, read_event_queue)

    def direct_write(self, req_id, sub_req_id, offset, length, data, timestamp):
        # self.path_queue.append(
        #         (self.volume_id, self.segment_id, req_id, offset, length, timestamp, 'direct'))
        # self.write_event_queue.append((self.segment_id, Event()))
        result = self.request_processor.write(req_id, sub_req_id, offset, length, data, timestamp)
    
    def buffer_write(self, req_id, sub_req_id, offset, length, data, timestamp):
        # self.path_queue.append(
        #         (self.volume_id, self.segment_id, req_id, offset, length, timestamp, 'buffer'))
        log_id, offset_, length, file_id, file_offset, buffer_server_id = self.buffer_processor.buffer_write(self.segment_id, req_id, sub_req_id, offset, length, data, timestamp, self.index)
        self.index.insert(log_id, offset, length, file_id, file_offset, buffer_server_id)
        return

    def direct_read(self, req_id, sub_req_id, offset, length, timestamp):
        # self.path_queue.append(
        #         (self.volume_id, self.segment_id, req_id, offset, length, timestamp, 'direct'))
        # self.read_event_queue.append((self.segment_id, Event()))
        result = self.request_processor.read(req_id, sub_req_id, offset, length, timestamp)

    def buffer_read(self, req_id, sub_req_id, offset, length, log_id,  file_id, file_offset, buffer_server_id, timestamp, on_read_obsolete, paras):
        # self.path_queue.append(
        #         (self.volume_id, self.segment_id, req_id, offset, length, timestamp, 'buffer'))
        result = self.buffer_processor.buffer_read(self.segment_id, req_id, sub_req_id, log_id, offset, length, file_id, file_offset, buffer_server_id, timestamp, on_read_obsolete, paras)
    
    def on_read_obsolete(self, paras, flag):
        req_id, sub_req_id, offset, length, index_node = paras
        # logging.info(paras)
        if flag == 2:
            index_node.is_obsolete = True
        else:
            pass
        temp_time = time.time()*1000000
        self.read(req_id, sub_req_id, offset, length, temp_time)
    
    def update_index(self):
        self.current_log_head = 0
        try:
            while True:
                log_head = self.request_processor.get_log_head()
                # logging.info('########################################')
                # logging.info('log head: {0}'.format(log_head))
                # logging.info('valid log node num: {0}'.format(self.index.logNodeNum(0)))
                # logging.info('all log node num: {0}'.format(self.index.treeNodeNum(self.index.root)))
                # logging.info('tree node num: {0}'.format(self.index.logNodeNum(1)))
                if log_head != -1:
                    self.index.rmLog(log_head, self.busy_flag)
                time.sleep(5*random.random())
        except Exception as err:
            logging.info(err)

    def merge_index(self):
        try:
            self.index.async_merge_log_to_tree(self.busy_flag)
            pass
        except Exception as err:
            logging.info(err)

    def monitor_index(self):
        try:
            self.node_num_list = list()
            while True:
                node_num = self.index.treeNodeNum(self.index.root)
                self.node_num_list.append(node_num)
                time.sleep(5)
        except Exception as err:
            logging.info(err)

    def get_left_range(self, offset, length, ranges):
        range_list = list()
        if len(ranges) == 0:
            if 0 != length:
                range_list.append((offset, length))
            return range_list
        offset2, length2 = ranges[0][0], ranges[0][1]
        if offset < offset2:
            range_list.append((offset, offset2-offset))
        return range_list + self.get_left_range(offset2+length2, offset+length-(offset2+length2), ranges[1:])

    def simple_write(self, req_id, sub_req_id, offset, length, data, timestamp):
        self.load_recorder.record_req(length)
        # self.load_recorder.record_stable_bandwidth(length)
        self.direct_write(req_id, sub_req_id, offset, length, data, timestamp)
    
    def log_write(self, req_id, sub_req_id, offset, length, data, timestamp):
        self.load_recorder.record_req(length)
        # self.load_recorder.record_stable_bandwidth(length)
        self.buffer_write(req_id, sub_req_id, offset, length, data, timestamp)

    def write(self, req_id, sub_req_id, offset, length, data, timestamp, state):
        # self.load_recorder.record_req(length)
        # self.load_recorder.record_stable_bandwidth(length)
        if state == 0:
            self.load_recorder.record_req(length)
            # if self.index.query_first(self.index.root, offset, length) != self.index.null:
            if self.index.range_overlap(offset, length):
                self.buffer_write(req_id, sub_req_id, offset, length, data, timestamp)
            else:
                self.direct_write(req_id, sub_req_id, offset, length, data, timestamp)
        else:
            self.buffer_write(req_id, sub_req_id, offset, length, data, timestamp)

    def read(self, req_id, sub_req_id, offset, length, timestamp):
        self.load_recorder.record_req(length)
        # self.load_recorder.record_stable_bandwidth(length)
        buffer_list = self.index.search(offset, length)
        for b in buffer_list:
            self.buffer_read(req_id, sub_req_id, b[0], b[1], b[2], b[3], b[4], b[6], timestamp, self.on_read_obsolete, (req_id, sub_req_id, b[0], b[1], b[5]))
        try:
            direct_list = self.get_left_range(offset, length, buffer_list)
            for d in direct_list:
                self.direct_read(req_id, sub_req_id, d[0], d[1], timestamp)
        except Exception as err:
            logging.info('offset{0}, length{1}, {2}'.format(offset, length, err))

    def output_applier_log(self):
        output_fd = open(self.applier_log_path, 'a')
        if os.path.getsize(self.applier_log_path) == 0:
            output_fd.write('disk,segment,type,peak_id,value,time,time_cost\n')
        while len(self.applier_queue)!=0:
            application = self.applier_queue.popleft()
            for i in range(0, 7):
                output_fd.write(str(application[i]))
                if i != 6:
                    output_fd.write(',')
            output_fd.write('\n')
        output_fd.flush()
        output_fd.close()

    def output_path_log(self):
        output_fd = open(self.path_log_path, 'a')
        if os.path.getsize(self.path_log_path) == 0:
            output_fd.write('disk,segment,req id,offset,length,time,type\n')
        path_list = list(self.path_queue)
        path_list.sort(key=lambda elem: elem[5])
        for req in path_list:
            for i in range(0, 7):
                output_fd.write(str(req[i]))
                if i != 6:
                    output_fd.write(',')
            output_fd.write('\n')
        output_fd.flush()
        output_fd.close()
    
    def output_index_records(self):
        path = '/home/k8s/wzhzhu/index_node_num_' + str(self.volume_id) + '.csv'
        output_fd = open(path, 'a')
        if os.path.getsize(path) == 0:
            output_fd.write('index node num\n')
        for num in self.node_num_list:
            output_fd.write(str(num))
            output_fd.write('\n')
        output_fd.write('-1')
        output_fd.write('\n')
        output_fd.flush()
        output_fd.close()

    def output_log(self):
        # self.output_applier_log()
        # self.output_path_log()
        # self.output_index_records()
        self.request_processor.output_log()
        self.buffer_processor.output_log()
