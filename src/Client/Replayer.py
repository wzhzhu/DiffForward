#!/usr/bin/python2
#coding:utf-8
from gevent import monkey;monkey.patch_all()
import socket
import gevent
from gevent.event import Event
import time
from ..Utils.DataTrans import *
import pandas as pd
import logging
import sys
import Client
import ReplicaClient
import collections
import psutil, os
import getopt
import struct
from ..Properties.config import *
import traceback
import numpy as np
import BurstDetector
import BufferClient
import gc
gc.disable()


class BusyFlag:
    def __init__(self):
        self.busy_num = 0
        self.idle_event = Event()

    def add(self):
        self.busy_num += 1
        self.idle_event.clear()

    def sub(self):
        self.busy_num -= 1
        if self.busy_num == 0:
            self.idle_event.set()

    def lock(self):
        self.busy_num = 1
    
    def unlock(self):
        self.busy_num = 0
    
    def isbusy(self):
        if self.busy_num != 0:
            return True
        return False
    
    def wait(self):
        self.idle_event.wait()

class Replayer:
    '''**********************************************************************************************
        server init
    **********************************************************************************************'''
    def __init__(self, replayer_id, manager_ip, manager_port, segmentation, report_gap, writing_mode, buffer_writing_manner, strip_length, seg_num, proxy_number, replication):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        self.segments = dict()
        self.segment_2_req_server = dict()
        self.manager_ip = manager_ip 
        self.manager_port = int(manager_port)
        self.segment_clients = dict()
        self.replayer_id = int(replayer_id)
        self.report_gap = int(report_gap)
        self.segmentation = int(segmentation)
        self.writing_mode = writing_mode
        self.buffer_writing_manner = buffer_writing_manner
        self.strip_length = int(strip_length)
        self.seg_num = int(seg_num)
        self.replication = int(replication)
        logging.info('writing_mode {0}'.format(writing_mode))
        logging.info('buffer_writing_manner {0}'.format(buffer_writing_manner))
        self.busy_flag = BusyFlag()
        self.proxy_number = int(proxy_number)
        self.burst_detector_dict = dict()
        self.set_manager()
        self.set_segment()
        if self.replication == 0:
            self.set_buffer_server()
        self.set_trace_path()
        self.set_log_path()
        self.load_trace(self.trace_path)
        self.create_client()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.server_socket.bind(('0.0.0.0', 0))
        self.replayer_port = self.server_socket.getsockname()[1]
        self.server_socket.listen(1000000)
        self.notify_replayer_ready()

    def set_manager(self):
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.manager_socket.connect((self.manager_ip,self.manager_port))

    def set_segment(self):
        request = struct.pack('>B',GET_SEGMENT_INFO)
        request += struct.pack('>I', self.replayer_id)
        send(request, self.manager_socket)
        response = recv(self.manager_socket)
        req_len = len(response)
        start = 0
        while start < req_len:
            volume_id, segment_id = struct.unpack('>II',response[start: start+8])
            if volume_id not in self.burst_detector_dict:
                self.burst_detector_dict[volume_id] = BurstDetector.BurstDetector(volume_id)
            start += 8
            replica_num = struct.unpack('>I',response[start:start+4])[0]
            start += 4
            if replica_num == 1:
                ip_len = struct.unpack('>I',response[start:start+4])[0]
                start += 4
                ip = response[start:start+ip_len].decode(encoding='utf-8')
                start += ip_len
                port = struct.unpack('>I',response[start:start+4])[0]
                start += 4
                self.segment_2_req_server[(volume_id,segment_id)] = (ip, port)
                self.replication = 0
            else:
                req_server_list = list()
                for i in range(0, replica_num):
                    ip_len = struct.unpack('>I',response[start:start+4])[0]
                    start += 4
                    ip = response[start:start+ip_len].decode(encoding='utf-8')
                    start += ip_len
                    port = struct.unpack('>I',response[start:start+4])[0]
                    start += 4
                    req_server_list.append((ip, port))
                self.segment_2_req_server[(volume_id,segment_id)] = req_server_list
                self.replication = 1
        logging.info(self.segment_2_req_server)
        self.segments = list(self.segment_2_req_server.keys())

    def set_buffer_server(self):
        self.buffer_server_dict = dict()
        volume_set = set()
        for volume_id, segment_id in self.segments:
            volume_set.add(volume_id)
        for volume_id in volume_set:
            request = struct.pack('>BI',GET_BUFFER_PROXY_INFO, volume_id)
            send(request, self.manager_socket)
            response = recv(self.manager_socket)
            rsp_len = len(response)
            start = 0
            buffer_server_list = list()
            while start < rsp_len:
                ip_len = struct.unpack('>I',response[start:start+4])[0]
                start += 4
                ip = response[start:start+ip_len].decode(encoding='utf-8')
                start += ip_len
                port = struct.unpack('>I', response[start:start+4])[0]
                start += 4
                buffer_server_list.append((ip, port))
            self.buffer_server_dict[volume_id] = buffer_server_list
            # logging.info('volume {0} buffer server(ip, port)'.format(volume_id))
            logging.info(buffer_server_list)

    def set_trace_path(self):
        request = struct.pack('>B',GET_TRACE_PATH)
        send(request, self.manager_socket)
        response = recv(self.manager_socket)
        trace_path = response.decode(encoding='utf-8')
        self.trace_path = trace_path
        logging.info(self.trace_path)

    def set_log_path(self):
        request = struct.pack('>B',GET_LOG_PATH)
        send(request, self.manager_socket)
        response = recv(self.manager_socket)
        log_path = response.decode(encoding='utf-8')
        self.log_path = log_path
        logging.info(self.log_path)
    
    def notify_replayer_ready(self):
        request = struct.pack('>BII',NOTIFY_REPLAYER_READY, self.replayer_id, self.replayer_port)
        send(request, self.manager_socket)
    
    def load_trace(self, trace_path):
        volumes = set()
        for volume_id, segment_id in self.segments:
            volumes.add(volume_id)
        trace_list = list()
        self.req_num = dict()
        for volume in volumes:
            volume_trace_path = trace_path+'volume_'+str(volume)+'.csv'
            # volume_trace_path = trace_path+'big_disk.csv'
            self.req_num[volume] = 0
            if os.path.isfile(volume_trace_path):
                volume_trace = pd.read_csv(volume_trace_path)
                self.req_num[volume] = len(volume_trace)
                trace_list.append(volume_trace)
        if len(trace_list)!=0:
            trace_df = pd.concat(trace_list,ignore_index=True)
            trace_df = trace_df.sort_values(by=['time'], ascending = [True], kind = 'mergesort')
            op_list = trace_df['op'].tolist()
            # op_list = ['W']*len(op_list)
            volume_list = trace_df['disk'].tolist()
            offset_list = trace_df['offset'].tolist()
            length_list = trace_df['length'].tolist()
            # length_list = [4096]*len(self.length_list)
            time_list = trace_df['time'].tolist()

            # self.op_list = collections.deque(op_list)
            # self.volume_list = collections.deque(volume_list)
            # self.offset_list = collections.deque(offset_list)
            # self.length_list = collections.deque(length_list)
            # self.time_list = trace_df['time'].tolist()
            self.op_list = op_list
            self.volume_list = volume_list
            self.offset_list = offset_list
            self.length_list = length_list
            self.time_list = time_list
            self.total_req_num = len(self.time_list)
        else:
            self.total_req_num = 0
        logging.info('{0} requests in total'.format(self.total_req_num))
        self.time_log = collections.deque()
        self.data_seg = bytearray('8'*1024*1024, encoding='utf-8')
        self.data_seg = np.array(self.data_seg)

    def create_client(self):
        volume_set = set()
        bf_dict = dict()
        for volume_id, segment_id in self.segments:
            volume_set.add(volume_id)

        self.write_event_queue = collections.deque()
        self.read_event_queue = collections.deque()
        if self.replication == 0:
            for volume_id in volume_set:
                bf = BufferClient.BufferClient(volume_id, self.manager_ip, self.manager_port, self.buffer_server_dict[volume_id], self.buffer_writing_manner, self.busy_flag)
                bf_dict[volume_id] = bf

            for volume_id, segment_id in self.segments:
                # if segment_id != 0:
                #     continue
                client = Client.Client(volume_id, segment_id, self.log_path, self.busy_flag)
                client.bind_req_server(self.segment_2_req_server[(volume_id, segment_id)][0], self.segment_2_req_server[(volume_id, segment_id)][1], self.manager_ip, self.manager_port, self.busy_flag)
                client.bind_buffer_server(bf_dict[volume_id])
                # client.bind_buffer_server(self.manager_ip, self.manager_port, self.buffer_server_dict[volume_id])
                client.set_load_recorder(self.manager_ip, self.manager_port, self.report_gap)
                client.set_req_event_queue(self.write_event_queue, self.read_event_queue)
                self.segment_clients[(volume_id, segment_id)] = client
        else:
            for volume_id, segment_id in self.segments:
                client = ReplicaClient.Client(volume_id, segment_id, self.log_path, self.busy_flag)
                client.set_proxy_number(self.proxy_number)
                client.bind_req_server(self.segment_2_req_server[(volume_id, segment_id)], self.manager_ip, self.manager_port, self.busy_flag)
                # client.bind_buffer_server(bf_dict[volume_id])
                # client.set_req_event_queue(self.write_event_queue, self.read_event_queue)
                self.segment_clients[(volume_id, segment_id)] = client
        logging.info('clients set')

    def run(self):
        self.serve()

    def serve(self):
        client_socket, client_addr = self.server_socket.accept()
        self.recv_request(client_socket, client_addr)

    def recv_request(self, client_socket, client_addr):
        try:
            request = recv(client_socket)
            if not request:
                logging.info("{} offline".format(client_addr))
                client_socket.close()
                return
            op = struct.unpack('>B', request[0:1])[0]
            if op == START_REPLAY:
                logging.info('start replay...')
                self.start_replay(request[1:], client_socket)
        except Exception as err:
            print('replayer id {0}'.format(self.replayer_id))
            print(traceback.extract_tb(sys.exc_info()[2]))
            print(sys.exc_info())
        finally:
            client_socket.close()


    def start_replay(self, request, client_socket):
        start = 0
        start_sys_time_len = struct.unpack('>I',request[start:start+4])[0]
        start += 4
        start_sys_time = request[start:start+start_sys_time_len].decode(encoding='utf-8')
        start_sys_time = float(start_sys_time)
        start += start_sys_time_len

        start_trace_time_len = struct.unpack('>I',request[start:start+4])[0]
        start += 4
        start_trace_time = request[start:start+start_trace_time_len].decode(encoding='utf-8')
        start_trace_time = float(start_trace_time)
        start += start_trace_time_len
        
        end_trace_time_len = struct.unpack('>I',request[start:start+4])[0]
        start += 4
        end_trace_time = request[start:start+end_trace_time_len].decode(encoding='utf-8')
        end_trace_time = float(end_trace_time)

        localtime = time.asctime(time.localtime(start_sys_time/1000000))
        tracetime = time.asctime(time.localtime(start_trace_time/1000000))
        logging.info ('trace replaying time {0}'.format(tracetime))
        traceendtime = time.asctime(time.localtime(end_trace_time/1000000))
        logging.info ('replaying to be ended at {0}'.format(traceendtime))

        self.replay(start_sys_time, start_trace_time, end_trace_time - start_trace_time)
        time.sleep(10)
        if self.total_req_num != 0:
            self.output()

    def split(self, offset, length, data, strip_length):
        start_strip_id = int(offset/strip_length)
        end_strip_id = int((offset+length-1)/strip_length)
        result_list = list()
        data_offset = 0
        if start_strip_id == end_strip_id: 
            result_list.append((offset,length,data))
            return result_list
        result_list.append((offset,(start_strip_id+1)*strip_length - offset, data[0:(start_strip_id+1)*strip_length - offset]))
        data_offset += (start_strip_id+1)*strip_length - offset
        for i in range(start_strip_id + 1, end_strip_id):
            result_list.append((i*strip_length, strip_length, data[data_offset: data_offset+strip_length]))
            data_offset += strip_length
        result_list.append((end_strip_id*strip_length, offset+length-end_strip_id*strip_length, data[data_offset: data_offset+offset+length-end_strip_id*strip_length]))
        return result_list

    def split_read(self, offset, length, strip_length):
        start_strip_id = int(offset/strip_length)
        end_strip_id = int((offset+length-1)/strip_length)
        result_list = list()
        data_offset = 0
        if start_strip_id == end_strip_id: 
            result_list.append((offset,length))
            return result_list
        result_list.append((offset,(start_strip_id+1)*strip_length - offset))
        for i in range(start_strip_id + 1, end_strip_id):
            result_list.append((i*strip_length, strip_length))
        result_list.append((end_strip_id*strip_length, offset+length-end_strip_id*strip_length))
        return result_list
    
    def volume_offset_2_segment_id(self, offset, strip_length, seg_num):
        return int(offset//(128*1024*1024*1024))*seg_num + int(((offset%(128*1024*1024*1024))%(seg_num*strip_length))//(strip_length))

    def volume_offset_2_segment_offset(self, offset, strip_length, seg_num):
        return int((offset%(128*1024*1024*1024))//strip_length//seg_num)*strip_length+(offset%(128*1024*1024*1024))%strip_length

    def classify(self, volume_id, req_id, offset, length, timestamp):
        state_change = self.burst_detector_dict[volume_id].put_wreq(offset, length, timestamp)
        return state_change

    def replay(self, start_sys_time, start_trace_time, replay_time_len):
        logging.info('request number: '+str(self.total_req_num))
        if self.total_req_num == 0:
            for volume_id, segment_id in self.segments:
                request = struct.pack('>BII',STOP_SEG_SCHEDULE,volume_id,segment_id)
                send(request,self.manager_socket)
            logging.info('sleep 20s')
            time.sleep(20)
            logging.info("I'm dead!")
            return
        batch_start_req_id = batch_end_req_id = 0
        time_gap = start_sys_time - start_trace_time
        strip_length = self.strip_length
        seg_num = self.seg_num
        if self.replication == 0:
            for volume_id, segment_id in self.segments:
                self.segment_clients[(volume_id, segment_id)].load_recorder.start(start_sys_time)
                gevent.spawn(self.segment_clients[(volume_id, segment_id)].update_index)
                gevent.spawn(self.segment_clients[(volume_id, segment_id)].merge_index)
                gevent.spawn(self.segment_clients[(volume_id, segment_id)].monitor_index)
        volume_req_id_dict = dict()
        while True:
            current_sys_time = time.time() * 1000000
            while batch_end_req_id < self.total_req_num and current_sys_time - start_sys_time >= self.time_list[
                batch_end_req_id] - start_trace_time:
                batch_end_req_id += 1
            for req_id in range(batch_start_req_id, batch_end_req_id):
                # current_sys_time = time.time() * 1000000
                current_sys_time = start_sys_time + self.time_list[req_id] - start_trace_time
                op = self.op_list[req_id]
                volume_id = self.volume_list[req_id]
                offset = self.offset_list[req_id]
                length = self.length_list[req_id]
                timestamp = self.time_list[req_id]
                data = self.data_seg[0:length]
                if volume_id not in volume_req_id_dict:
                    volume_req_id_dict[volume_id] = 0

                if op == 'W':
                    if self.writing_mode == 'hybrid':
                        state = self.classify(volume_id, volume_req_id_dict[volume_id], offset, length, current_sys_time)
                    if self.segmentation == 0:
                        req_list = self.split(offset,length,data,strip_length)
                        sub_req_id = 0
                        for req in req_list:
                            req_offset = req[0]
                            req_length = req[1]
                            req_data = req[2]
                            if self.writing_mode == 'direct':
                                self.segment_clients[(volume_id,0)].simple_write(volume_req_id_dict[volume_id], sub_req_id, req_offset, req_length, req_data, current_sys_time)
                            elif self.writing_mode == 'hybrid':
                                self.segment_clients[(volume_id,0)].write(volume_req_id_dict[volume_id], sub_req_id, req_offset, req_length, req_data, current_sys_time, state)
                            elif self.writing_mode == 'buffer':
                                self.segment_clients[(volume_id,0)].log_write(volume_req_id_dict[volume_id], sub_req_id, req_offset, req_length, req_data, current_sys_time)
                            sub_req_id += 1
                    else:
                        req_list = self.split(offset,length,data,strip_length)
                        sub_req_id = 0
                        for req in req_list:
                            req_offset = req[0]
                            req_length = req[1]
                            req_data = req[2]
                            segment_id = self.volume_offset_2_segment_id(req_offset,strip_length,seg_num)
                            segment_offset = req_offset
                            if self.writing_mode == 'direct':
                                self.segment_clients[(volume_id,segment_id)].simple_write(volume_req_id_dict[volume_id], sub_req_id, segment_offset, req_length, req_data, current_sys_time)
                            elif self.writing_mode == 'hybrid':
                                self.segment_clients[(volume_id,segment_id)].write(volume_req_id_dict[volume_id], sub_req_id, segment_offset, req_length, req_data, current_sys_time, state)
                            elif self.writing_mode == 'buffer':
                                self.segment_clients[(volume_id,segment_id)].log_write(volume_req_id_dict[volume_id], sub_req_id, segment_offset, req_length, req_data, current_sys_time)
                            sub_req_id += 1
                elif op =='R':
                    if self.segmentation == 0:
                        req_list = self.split_read(offset,length,strip_length)
                        sub_req_id = 0
                        for req in req_list:
                            req_offset = req[0]
                            req_length = req[1]
                            self.segment_clients[(volume_id,0)].read(volume_req_id_dict[volume_id], sub_req_id, req_offset, req_length, current_sys_time)
                            sub_req_id += 1
                    else:
                        req_list = self.split_read(offset,length,strip_length)
                        sub_req_id = 0
                        for req in req_list:
                            req_offset = req[0]
                            req_length = req[1]
                            segment_id = self.volume_offset_2_segment_id(req_offset,strip_length,seg_num)
                            segment_offset = req_offset
                            self.segment_clients[(volume_id,segment_id)].read(volume_req_id_dict[volume_id], sub_req_id, segment_offset, req_length, current_sys_time)
                            sub_req_id += 1
                volume_req_id_dict[volume_id] += 1
                # self.time_log.append((self.time_list[req_id],current_sys_time - time_gap, current_sys_time - time_gap - self.time_list[req_id]))
            batch_start_req_id = batch_end_req_id
            if batch_end_req_id == self.total_req_num:
                for volume_id, segment_id in self.segments:
                    request = struct.pack('>BII',STOP_SEG_SCHEDULE,volume_id,segment_id)
                    send(request,self.manager_socket)
                # logging.info('all requests are replayed over!, waiting...')
                while time.time()*1000000 < start_sys_time + replay_time_len:
                    time.sleep(1)
                # logging.info('replaying end!')
                # gc.enable()
                time.sleep(20)
                logging.info("I'm dead!")
                break
            # deciding sleep time
            temp_time = time.time() * 1000000
            sleep_time = self.time_list[batch_end_req_id] - start_trace_time - (temp_time - start_sys_time)
            if sleep_time > 10000:
                time.sleep((sleep_time/1000000)/2)
            elif sleep_time/1000000 < 0.002:
                time.sleep(0.0001)
                # gevent.idle()
            else:
                time.sleep(0.001)

    def output(self):
        try:
            logging.info('output log')
            for volume_id, segment_id in self.segment_clients:
                if self.req_num[volume_id] != 0:
                    logging.info('volume_id{0} segment_id{1}'.format(volume_id, segment_id))
                    self.segment_clients[(volume_id, segment_id)].output_log()
        except Exception as err:
            logging.info(err)

    def output_time_log(self, time_log_path):
        #replaying precision
        output_fd = open(time_log_path, 'w')
        output_fd.write('ideal time, replay time, difference\n')
        while len(self.time_log) != 0:
            application = self.time_log.popleft()
            for i in range(0, 3):
                output_fd.write(str(application[i]))
                if i != 2:
                    output_fd.write(',')
            output_fd.write('\n')
        output_fd.flush()
        output_fd.close() 

def main(argv):
    # p.nice(-20)
    replayer_id = manager_ip = manager_port = segmentation = report_gap = writing_mode = buffer_writing_manner = strip_length = seg_num = proxy_number = replication = None
    opts, args = getopt.getopt(argv,"hi:m:p:s:r:w:b:l:n:o:e:",["id=",'manager_ip=','manager_port=','segmentation=','report_gap=', 'writing_mode=', 'buffer_writing_manner=', 'strip_length=', 'seg_num=', 'proxy_number=', 'replication='])
    for opt, arg in opts:
        if opt == '-h':
            logging.info('Replayer.py -i <id>')
            sys.exit()
        elif opt in ("-i", "--id"):
            replayer_id = int(arg)
        elif opt in ("-m", "--manager_ip"):
            manager_ip = arg
        elif opt in ("-p", "--manager_port"):
            manager_port = arg
        elif opt in ("-s", "--segmentation"):
            segmentation = arg
        elif opt in ("-r", "--report_gap"):
            report_gap = arg
        elif opt in ("-w", "--writing_mode"):
            writing_mode = arg
        elif opt in ("-b", "--buffer_writing_manner"):
            buffer_writing_manner = arg
        elif opt in ("-l", "--strip_length"):
            strip_length = arg
        elif opt in ("-n", "--seg_num"):
            seg_num = arg
        elif opt in ("-o", "--proxy_number"):
            proxy_number = arg
        elif opt in ("-e", "--replication"):
            replication = arg
    my_replayer = Replayer(replayer_id, manager_ip, manager_port, segmentation, report_gap, writing_mode, buffer_writing_manner, strip_length, seg_num, proxy_number, replication)
    bind_cpu_core = False
    if bind_cpu_core:
        p = psutil.Process(os.getpid())
        count = psutil.cpu_count()
        cpu_list = p.cpu_affinity()
        target_cpu_core = cpu_list[replayer_id%len(cpu_list)]
        p.cpu_affinity([target_cpu_core])
    my_replayer.run()

if __name__ == "__main__":
    main(sys.argv[1:])
