#!/usr/bin/python3
#coding:utf-8
import sys
import gevent
from gevent import monkey
import socket
from Properties.config import *
import time
import getopt
from Utils.DataTrans import *
import struct
import psutil, os
import math
from ..Utils.ConfigReader import ConfigReader
import random
import paramiko
import logging
import pandas as pd
import math
import pickle

monkey.patch_all()
p = psutil.Process(os.getpid())

class Manager:
    def __init__(self):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        reader = ConfigReader()
        self.src = reader.get('Src Path','path')
        self.alpha = float(reader.get('Other Settings','alpha'))
        logging.info('alpha = {0}'.format(self.alpha))
        self.segment_pos = dict()
        self.init_socket()
        self.init_bandwidth_status()
        self.segmentation = int(reader.get('Other Settings','segmentation'))
        if self.segmentation == 0:
            self.no_segmentation()
        else:
            strip_length = int(reader.get('Other Settings','strip_length'))
            seg_num = int(reader.get('Other Settings','seg_num'))
            self.do_segmentation(strip_length, seg_num)
        self.start_schedule = 0
        self.schedule_count = 0
        gevent.spawn(self.schedule)
        self.stop_schedule_segment = set()
        self.buffer_server_num =int(reader.get('Buffer Servers','server_num'))

    def init_socket(self):
        reader = ConfigReader()
        self.manager_ip = reader.get('Manager','ip')
        self.manager_port = int(reader.get('Manager','port'))
        logging.info('manager_ip '+self.manager_ip)
        logging.info('manager_port '+str(self.manager_port))
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.server_socket.bind(('0.0.0.0', self.manager_port))
        self.server_socket.listen(1000000)
        logging.info('################################listening socket init finish!')

    def init_bandwidth_status(self):
        reader = ConfigReader()
        self.bandwidth_max = int(reader.get('Other Settings','bandwidth_max'))
        buffer_server_num = int(reader.get('Buffer Servers','server_num'))
        self.bandwidth_stable_used = dict()
        for i in range(0, buffer_server_num):
            self.bandwidth_stable_used[i] = 0
        self.stable_bandwidth_dict = dict()
        gevent.spawn(self.print_stable_bandwidth)

    def do_segmentation(self, strip_length, seg_num):
        logging.info('do segmentation')
        reader = ConfigReader()
        trace_path = reader.get('Manager','trace_path')
        volume_start = int(reader.get('Volumes','start'))
        volume_end = int(reader.get('Volumes','end'))
        volume_num = volume_end - volume_start
        volume_list = list(range(volume_start, volume_end))
        # volume_list = [0]
        self.volume_segment = dict()
        for volume in volume_list:
            logging.info('volume'+str(volume))
            self.volume_segment[volume] = set()
            volume_trace_path = trace_path+'volume_'+str(volume)+'.csv'
            if not os.path.isfile(volume_trace_path):
                self.volume_segment[volume].add(0)
                continue
            volume_trace = pd.read_csv(volume_trace_path)
            offset_list = volume_trace['offset'].tolist()
            length_list = volume_trace['length'].tolist()
            req_num = len(offset_list)
            for i in range(0,req_num):
                offset = offset_list[i]
                length = length_list[i]
                start_strip = int(offset/strip_length)
                end_strip = int((offset+length-1)/strip_length)
                for j in range(start_strip, end_strip+1):
                    segment_id = self.offset_2_segment(j*strip_length,strip_length,seg_num)
                    self.volume_segment[volume].add(segment_id)
        output_fd = open('/home/k8s/wzhzhu/DiffForward/segmentation result', 'w')
        for volume in volume_list:
            for segment in self.volume_segment[volume]:
                output_fd.write(str(segment))
                output_fd.write(',')
            output_fd.write('\n')
        output_fd.flush()
        output_fd.close()
        self.segments = list()
        for volume in self.volume_segment:
            for segment in self.volume_segment[volume]:
                self.segments.append((volume, segment))
        logging.info('do segmentation end')

    def no_segmentation(self):
        reader = ConfigReader()
        volume_start = int(reader.get('Volumes','start'))
        volume_end = int(reader.get('Volumes','end'))
        volume_num = volume_end - volume_start
        volume_list = list(range(volume_start, volume_end))
        # volume_list = [1]
        self.segments = list()
        for volume in volume_list:
            self.segments.append((volume, 0))
    
    def offset_2_segment(self,offset, strip_length, seg_num):
        return int(offset//(128*1024*1024*1024))*seg_num + int(((offset%(128*1024*1024*1024))%(seg_num*strip_length))//strip_length)

    def run(self):
        self.serve()

    def serve(self):
        while True:
            client_socket, client_addr = self.server_socket.accept()
            gevent.spawn(self.recv_application, client_socket, client_addr)

    def recv_application(self, client_socket, client_addr):
        try:
            while True:
                request = recv(client_socket)
                if not request:
                    logging.info("{} offline".format(client_addr))
                    client_socket.close()
                    break
                op = struct.unpack('>B', request[0:1])[0]
                if op == INIT_BUFFER_PROCESS:
                    logging.info('init buffer process...')
                    self.init_buffer_process(request[1:], client_socket)
                elif op == INIT_FORWARDING_PROCESS:
                    logging.info('init forwarding process...')
                    reader = ConfigReader()
                    replication = int(reader.get('Other Settings','replication'))
                    if replication == 0:
                        self.init_forwarding_process(request[1:], client_socket)
                    else:
                        self.init_replica_forwarding_process(request[1:], client_socket)
                elif op == NOTIFY_FORWARDING_PROCESS_PORT:
                    # logging.info('notify forwarding process port...')
                    self.notify_forwarding_process_port(request[1:], client_socket)
                elif op == NOTIFY_FORWARDING_PROXY_PORT:
                    # logging.info('notify forwarding proxy port...')
                    reader = ConfigReader()
                    replication = int(reader.get('Other Settings','replication'))
                    if replication == 0:
                        self.notify_forwarding_proxy_port(request[1:], client_socket)
                    else:
                        self.notify_replica_forwarding_proxy_port(request[1:], client_socket)
                elif op == INIT_LOG_MERGER:
                    logging.info('init log merger...')
                    reader = ConfigReader()
                    replication = int(reader.get('Other Settings','replication'))
                    if replication == 0:
                        self.init_log_merger(request[1:], client_socket)
                    else:
                        logging.info('replication mode, cancel activating log merger!')
                        response = struct.pack('>B', SUCCEED)
                        send(response, client_socket)
                elif op == INIT_REPLAYER:
                    logging.info('init replayer...')
                    self.init_replayer(request[1:], client_socket)
                elif op == NOTIFY_REPLAYER_READY:
                    # logging.info('notify replayer ready...')
                    self.notify_replayer_ready(request[1:], client_socket)
                elif op == START_REPLAYER:
                    logging.info('start replayer...')
                    self.start_replayer(request[1:], client_socket)
                elif op == GET_SEGMENT_INFO:
                    # logging.info('get segment info...')
                    reader = ConfigReader()
                    replication = int(reader.get('Other Settings','replication'))
                    if replication == 0:
                        self.get_segment_info(request[1:], client_socket)
                    else:
                        self.get_replica_segment_info(request[1:], client_socket)
                elif op == GET_LOG_MERGER_PORT:
                    # logging.info('get log merger port...')
                    self.get_log_merger_port(request[1:], client_socket)
                elif op == GET_BUFFER_PROXY_INFO:
                    # logging.info('get buffer proxy info...')
                    self.get_buffer_proxy_info(request[1:], client_socket)
                elif op == GET_TRACE_PATH:
                    # logging.info('get trace path...')
                    self.get_trace_path(request[1:], client_socket)
                elif op == GET_LOG_PATH:
                    # logging.info('get log path...')
                    self.get_log_path(request[1:], client_socket)
                elif op == GET_REPLAYER_PORT:
                    # logging.info('get replayer port...')
                    self.get_replayer_port(request[1:], client_socket)
                elif op == GET_FORWARDING_PROXY_INFO:
                    # logging.info('get forwarding proxy info...')
                    self.get_forwarding_proxy_info(request[1:], client_socket)
                elif op == SHUTDOWN_BUFFER_PROCESS:
                    logging.info('shutdown buffer process...')
                    self.shutdown_buffer_process(request[1:], client_socket)
                elif op == SHUTDOWN_REQ_PROCESS: 
                    logging.info('shutdown forwarding process...')
                    self.shutdown_forwarding_process(request[1:], client_socket)
                elif op == SHUTDOWN_LOG_MERGER:
                    logging.info('shutdown log merger...')
                    self.shutdown_log_merger(request[1:], client_socket)
                elif op == GET_SERVER_SEGMENT_INFO:
                    # logging.info('get server segment info...')
                    self.get_server_segment_info(request[1:], client_socket)
                elif op == REPORT_LOAD:
                    # logging.info('report load...')
                    self.report_load(request[1:], client_socket)
                elif op == NOTIFY_BUFFER_PROCESS_PORT:
                    self.notify_buffer_process_port(request[1:], client_socket)
                elif op == NOTIFY_BUFFER_PROXY_PORT:
                    self.notify_buffer_proxy_port(request[1:], client_socket)
                elif op == RESET_SYS:
                    self.reset_sys(request[1:], client_socket)
                elif op == RESTRICT_BANDWIDTH:
                    self.restrict_bandwidth(request[1:], client_socket)
                elif op == UNRESTRICT_BANDWIDTH:
                    self.unrestrict_bandwidth(request[1:], client_socket)
                elif op == CLEAN_SYS:
                    self.clean_sys(request[1:], client_socket)
                elif op == STOP_SEG_SCHEDULE:
                    self.stop_seg_schedule(request[1:], client_socket)
                elif op == REPORT_STABLE_BANDWIDTH:
                    self.report_stable_bandwidth(request[1:], client_socket)
                elif op == GET_FREE_BANDWIDTH:
                    self.get_free_bandwidth(request[1:], client_socket)
                elif op == GET_SCHEDULE_COUNT:
                    self.get_schedule_count(request[1:], client_socket)
                elif op == NOTIFY_STORAGE_SERVER_ID:
                    self.notify_storage_server_id(request[1:], client_socket)
                elif op == INIT_STORAGE_SERVER:
                    self.init_storage_server(request[1:], client_socket)
                elif op == INIT_MERGE_SPEED_CONTROLLER:
                    self.init_merge_speed_controller(request[1:], client_socket)
                elif op == SHUTDOWN_MANAGER:
                    self.shutdown_manager(request[1:], client_socket)
        # except Exception as err:
        #     logging.info("recv_data error:{}".format(err))
        except Exception as err:
            client_socket.close()

    def start_process(self, ip, cmd):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
        ssh.connect(ip,22,'k8s')
        stdin,stdout,stderr = ssh.exec_command(cmd)
        ssh.close()

    def start_processes(self, ip, cmd_list):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
        ssh.connect(ip,22,'k8s')
        for cmd in cmd_list:
            flag = 0
            try:
                stdin,stdout,stderr = ssh.exec_command(cmd)
                # outmsg,errmsg = stdout.read(),stderr.read()
                # logging.info(errmsg)
                # time.sleep(0.001)
                flag = 1
            except Exception as err:
                logging.info('creating process fail{0}'.format(err))
        ssh.close()

    '''check done'''
    def start_process2(self, ip, cmd):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
        ssh.connect(ip,22,'k8s')
        # logging.info('ip '+ip+ ' '+cmd)
        stdin,stdout,stderr = ssh.exec_command(cmd)
        outmsg,errmsg = stdout.read(),stderr.read()
        # outmsg = str(outmsg)
        # logging.info(outmsg.replace("\\n","\\r\\n"))
        # logging.info(outmsg.decode())
        # if errmsg != b'':
        logging.info(errmsg)
        # logging.info(outmsg)
        ssh.close()
    
    def get_schedule_count(self, request, client_socket):
        response = struct.pack('>I', self.schedule_count)
        send(response, client_socket)

    def init_merge_speed_controller(self, request, client_socket):
        reader = ConfigReader()
        server_num = int(reader.get('Servers','server_num'))
        logging.info('init merge speed controller')
        for i in range(0, server_num):
            server_ip = reader.get('Server'+str(i),'ip')
            cmd = 'ulimit -n 65535 && cd {0} && nohup python3 MergeSpeedController.py >> ../output/merge_speed_controller_output 2>> ../output/merge_speed_controller_output &'\
            .format(self.src)
            logging.info(cmd)
            self.start_process(server_ip, cmd)
        time.sleep(5)
        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def init_storage_server(self, request, client_socket):
        reader = ConfigReader()
        server_num = int(reader.get('Servers','server_num'))
        storage_server_num = int(reader.get('Servers','storage_server_num'))
        self.storage_server_port = dict()
        cmd_dict = dict()
        storage_server_id = 0
        self.server_id_2_storage_server_id = dict()
        logging.info('init storage server')
        for i in range(0, server_num):
            self.server_id_2_storage_server_id[i] = list()
            server_ip = reader.get('Server'+str(i),'ip')
            if server_ip not in cmd_dict:
                cmd_dict[server_ip] = list()
            for j in range(0, storage_server_num):
                cmd = 'ulimit -n 65535 && cd {0} && nohup python2 StorageServer.py -i {1} -p {2} -s {3} >> ../output/storage_server_error{3} 2>> ../output/storage_server_output{3} &'\
                .format(self.src, self.manager_ip, self.manager_port, storage_server_id)
                logging.info(cmd)
                cmd_dict[server_ip].append(cmd)
                self.server_id_2_storage_server_id[i].append(storage_server_id)
                storage_server_id += 1

        for ip in cmd_dict:
            self.start_processes(ip, cmd_dict[ip])

        while len(self.storage_server_port.keys()) < server_num*storage_server_num:
            logging.info('the number of storage servers linked: {0}'.format(len(self.storage_server_port.keys())))
            time.sleep(1)
        logging.info('finish')
        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def notify_storage_server_id(self, request, client_socket):
        storage_server_id, port = struct.unpack('>II',request[0:4+4])
        self.storage_server_port[storage_server_id] = port

    def get_storage_server(self, server_id):
        storage_servers = self.server_id_2_storage_server_id[server_id]
        storage_server_id = storage_servers[random.randint(0, len(storage_servers)-1)]
        return (storage_server_id, self.storage_server_port[storage_server_id])

    def create_forwarding_proxy(self, segment_list, target_server_id):
        for volume_id, segment_id in segment_list:
            self.segment_pos[(volume_id, segment_id)] = target_server_id
            self.forwarding_proxy_port[(volume_id, segment_id)] = -1
            storage_server_id, storage_server_port = self.get_storage_server(target_server_id)
            request = struct.pack('>BII', CREATE_PROXY, volume_id, segment_id)
            process_id = random.randint(0, self.forwarding_process_num - 1)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.forwarding_server_ip[target_server_id], self.forwarding_process_port[(target_server_id, process_id)]))
            send(request, s)
            s.close()

    def init_replica_forwarding_process(self, request, client_socket):
        reader = ConfigReader()
        self.forwarding_server_num = int(reader.get('Servers','server_num'))
        self.forwarding_process_num = int(reader.get('Servers','process_num'))
        self.with_ceph = int(reader.get('Other Settings','with_ceph'))
        replica_manager_port = int(reader.get('Servers','replica_manager_port'))
        self.forwarding_server_ip = dict()
        for i in range(0, self.forwarding_server_num):
            forwarding_server_ip = reader.get('Server'+str(i),'ip')
            self.forwarding_server_ip[i] = forwarding_server_ip
        logging.info('start forwarding process')
        cmd_dict = dict()
        for server_id in range(0, self.forwarding_server_num):
            ip = self.forwarding_server_ip[server_id]
            if ip not in cmd_dict:
                cmd_dict[ip] = list()
            cmd = 'cd {0} && nohup python3 ReplicaForwardingProcessManager.py -p {1} >> ../output/replica_forwarding_process_manager_output 2>> ../output/replica_forwarding_process_manager_output &'\
                .format(self.src, replica_manager_port)
            cmd_dict[ip].append(cmd)
            logging.info(cmd)
        for ip in cmd_dict:
            self.start_processes(ip, cmd_dict[ip])
        time.sleep(5)

        self.forwarding_process_port = dict()
        for server_id in range(0, self.forwarding_server_num):
            parameters = list()
            for process_id in range(0, self.forwarding_process_num):
                storage_server_id, storage_server_port = self.get_storage_server(server_id)
                parameters.append((server_id, process_id, self.manager_ip, self.manager_port, self.with_ceph, storage_server_port, storage_server_id))
            request = pickle.dumps(parameters)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ip = self.forwarding_server_ip[server_id]
            s.connect((ip, replica_manager_port))
            send(request, s)
            s.close()

        while len(self.forwarding_process_port.keys()) < self.forwarding_server_num*self.forwarding_process_num:
            logging.info('forwarding process number: {0}'.format(len(self.forwarding_process_port.keys())))
            time.sleep(1)

        random.seed(1996)
        segments = list(self.segments)
        self.segment_load = dict()
        self.segment_process_id = dict()
        self.forwarding_proxy_port = dict()
        replica_num = int(reader.get('Servers','replica_num'))
        for volume_id, segment_id in segments:
            position = random.sample(range(0, self.forwarding_server_num), replica_num)
            self.segment_pos[(volume_id, segment_id)] = position
            process_id = random.randint(0, self.forwarding_process_num - 1)
            self.segment_process_id[(volume_id, segment_id)] = process_id
            self.segment_load[(volume_id, segment_id)] = 0
            replica_info = bytearray()
            replica_info += struct.pack('>I',replica_num)
            for i in range(0, replica_num):
                replica_info += struct.pack('>I',len(self.forwarding_server_ip[position[i]]))
                replica_info += bytearray(self.forwarding_server_ip[position[i]], encoding='utf-8')
                replica_info += struct.pack('>I', self.forwarding_process_port[(position[i], process_id)])
            for i in range(0, replica_num):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.forwarding_server_ip[position[i]], self.forwarding_process_port[(position[i], process_id)]))
                request = struct.pack('>BIII', CREATE_PROXY, volume_id, segment_id, i)
                request += replica_info
                send(request, s)
                s.close()
                # logging.info('volume {0} segment {1} forwarding process id {2}'.format(volume_id, segment_id, process_id))
        while len(self.forwarding_proxy_port.keys()) < len(self.segments)*replica_num:
            logging.info('forwarding proxy number{0}'.format(len(self.forwarding_proxy_port.keys())))
            time.sleep(1)

        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def notify_replica_forwarding_proxy_port(self, request, client_socket):
        volume_id, segment_id, replica_id, port = struct.unpack('>IIII', request[0:4+4+4+4])
        if (volume_id, segment_id, replica_id) not in self.forwarding_proxy_port or self.forwarding_proxy_port[(volume_id, segment_id, replica_id)] == -1:
            self.forwarding_proxy_port[(volume_id, segment_id, replica_id)] = port
        else:
            logging.info('notify port error!')

    def init_forwarding_process(self, request, client_socket):
        reader = ConfigReader()
        self.forwarding_server_num = int(reader.get('Servers','server_num'))
        self.forwarding_process_num = int(reader.get('Servers','process_num'))
        self.with_ceph = int(reader.get('Other Settings','with_ceph'))
        self.forwarding_server_ip = dict()
        for i in range(0, self.forwarding_server_num):
            forwarding_server_ip = reader.get('Server'+str(i),'ip')
            self.forwarding_server_ip[i] = forwarding_server_ip
        logging.info('start forwarding process')
        cmd_dict = dict()
        for server_id in range(0, self.forwarding_server_num):
            for process_id in range(0, self.forwarding_process_num):
                ip = self.forwarding_server_ip[server_id]
                if ip not in cmd_dict:
                    cmd_dict[ip] = list()
                storage_server_id, storage_server_port = self.get_storage_server(server_id)
                cmd = 'ulimit -n 65535 && cd {0} && nohup python3 ForwardingProcess.py -f {1} -r {2} -i {3} -p {4} -w {5} -t {6} -d {7} >> ../output/forwarding_process_errlog{2} 2>> ../output/forwarding_process_output{2} &'\
                .format(self.src, server_id, process_id, self.manager_ip, self.manager_port, self.with_ceph, storage_server_port, storage_server_id)
                if ip not in cmd_dict:
                    cmd_dict[ip] = list()
                cmd_dict[ip].append(cmd)
                logging.info('ip'+ ip + cmd)
        self.forwarding_process_port = dict()
        for ip in cmd_dict:
            self.start_processes(ip, cmd_dict[ip])
        while len(self.forwarding_process_port.keys()) < self.forwarding_server_num*self.forwarding_process_num:
            logging.info('forwarding process number: {0}'.format(len(self.forwarding_process_port.keys())))
            time.sleep(1)

        random.seed(1996)
        segments = list(self.segments)
        random.shuffle(segments)
        position = 0
        # self.segment_pos = dict()
        self.segment_load = dict()
        self.segment_process_id = dict()
        self.forwarding_proxy_port = dict()
        for volume_id, segment_id in segments:
            self.segment_pos[(volume_id, segment_id)] = position
            process_id = random.randint(0, self.forwarding_process_num - 1)
            self.segment_process_id[(volume_id, segment_id)] = process_id
            self.segment_load[(volume_id, segment_id)] = 0
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.forwarding_server_ip[position], self.forwarding_process_port[(position, process_id)]))
            request = struct.pack('>BII', CREATE_PROXY, volume_id, segment_id)
            send(request, s)
            s.close()
            position = (position + 1)%self.forwarding_server_num
            logging.info('volume {0} segment {1} forwarding process id {2}'.format(volume_id, segment_id, process_id))
        while len(self.forwarding_proxy_port.keys()) < len(self.segments):
            logging.info('forwarding proxy number: {0}'.format(len(self.forwarding_proxy_port.keys())))
            time.sleep(1)

        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def notify_forwarding_process_port(self, request, client_socket):
        forwarding_server_id, process_id, port = struct.unpack('>III', request[0:4+4+4])
        if (forwarding_server_id, process_id) not in self.forwarding_process_port:
            self.forwarding_process_port[(forwarding_server_id, process_id)] = port
        else:
            logging.info('notify port error!')

    def notify_forwarding_proxy_port(self, request, client_socket):
        volume_id, segment_id, port = struct.unpack('>III', request[0:4+4+4])
        if (volume_id, segment_id) not in self.forwarding_proxy_port or self.forwarding_proxy_port[(volume_id, segment_id)] == -1:
            self.forwarding_proxy_port[(volume_id, segment_id)] = port
        else:
            logging.info('notify port error!')

    def init_buffer_process(self, request, client_socket):
        self.buffer_server_ip = dict()
        self.buffer_process_port = dict()
        buffer_server_ip_list = list()
        reader = ConfigReader()
        buffer_path = reader.get('Buffer Servers','buffer_path')
        buffer_server_num = int(reader.get('Buffer Servers','server_num'))
        buffer_process_num = int(reader.get('Buffer Servers','process_num'))
        for i in range(0, buffer_server_num):
            buffer_server_ip = reader.get('Buffer Server'+str(i),'ip')
            self.buffer_server_ip[i] = buffer_server_ip
            buffer_server_ip_list.append(buffer_server_ip)

        logging.info('start buffer process')
        cmd_dict = dict()
        for buffer_server_id in range(0, buffer_server_num):
            for buffer_process_id in range(0, buffer_process_num):
                ip = self.buffer_server_ip[buffer_server_id]
                cmd = 'ulimit -n 65535 && cd {0} && export CAIO_IMPL=python && nohup python3 BufferProcess.py -d {1} -i {2} -b {3} -n {4} -m {5} -p {6} > ../output/buffer_process_output{2} 2> ../output/buffer_process_output{2} &'\
                .format(self.src, buffer_server_id, buffer_process_id, buffer_path, buffer_server_num, self.manager_ip, self.manager_port)
                if ip not in cmd_dict:
                    cmd_dict[ip] = list()
                cmd_dict[ip].append(cmd)
                logging.info('ip'+ ip + cmd)
        self.buffer_process_port = dict()
        for ip in cmd_dict:
            self.start_processes(ip, cmd_dict[ip])
        while len(self.buffer_process_port.keys()) < buffer_server_num*buffer_process_num:
            logging.info(self.buffer_process_port)
            logging.info('buffer process number: {0}'.format(len(self.buffer_process_port)))
            time.sleep(1)
    
        volume_set = set()
        self.buffer_proxy_ip = dict()
        self.buffer_process_id = dict()
        self.buffer_proxy_port = dict()
        self.buffer_proxy_num = 0
        for volume_id, segment_id in self.segments:
            volume_set.add(volume_id)
        for volume_id in volume_set:
            self.buffer_proxy_ip[volume_id] = buffer_server_ip_list
            buffer_process_id = random.randint(0, buffer_process_num - 1)
            self.buffer_process_id[volume_id] = buffer_process_id
            for buffer_server_id in range(0, buffer_server_num):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.buffer_server_ip[buffer_server_id], self.buffer_process_port[(buffer_server_id, buffer_process_id)]))
                request = struct.pack('>BI', CREATE_ENGINE, volume_id)
                send(request, s)
                s.close()
        while self.buffer_proxy_num < len(volume_set)*buffer_server_num:
            logging.info('buffer proxy number: {0}'.format(self.buffer_proxy_num))
            time.sleep(1)

        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def notify_buffer_process_port(self, request, client_socket):
        buffer_server_id, buffer_process_id, port = struct.unpack('>III', request[0:4+4+4])
        if (buffer_server_id, buffer_process_id) not in self.buffer_process_port:
            self.buffer_process_port[(buffer_server_id, buffer_process_id)] = port
            # logging.info('buffer process {0} in buffer server {1} get port {2}'.format(buffer_process_id, buffer_server_id, port))
        else:
            logging.info('notify port error!')

    def notify_buffer_proxy_port(self, request, client_socket):
        volume_id, buffer_server_id, port = struct.unpack('>III', request[0:4+4+4])
        # logging.info(( volume_id, buffer_server_id, port))
        if (volume_id, buffer_server_id) not in self.buffer_proxy_port:
            self.buffer_proxy_port[(volume_id, buffer_server_id)] = port
            self.buffer_proxy_num += 1
        else:
            logging.info('notify port error!')

    def init_log_merger(self, request, client_socket):
        reader = ConfigReader()
        server_num = int(reader.get('Servers','server_num'))
        log_merger_port = int(reader.get('Log Merger','port'))
        inflight_merging_traffic_max = int(reader.get('Log Merger','inflight_merging_traffic_max'))
        merge_log = int(reader.get('Log Merger','merge_log'))
        for i in range(0, server_num):
            ip = self.forwarding_server_ip[i]
            cmd = 'ulimit -n 65535 && cd {0} && nohup sudo python3 LogMerger.py -i {1} -p {2} -m {3} -o {4} -t {5} -l {6} > ../output/log_merger_output 2> ../output/log_merger_output &'\
            .format(self.src, i, log_merger_port, self.manager_ip ,self.manager_port, inflight_merging_traffic_max, merge_log)
            logging.info(cmd)
            self.start_process(ip, cmd)
        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def shutdown_log_merger(self, request, client_socket):
        reader = ConfigReader()
        log_merger_port = int(reader.get('Log Merger','port'))
        for i in self.forwarding_server_ip:
            ip = self.forwarding_server_ip[i]
            port = log_merger_port
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, port))
            request = struct.pack('>B',SHUTDOWN)
            send(request, s)
            recv(s)
        # logging.info(s)
        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def stop_seg_schedule(self, request, client_socket):
        volume_id, segment_id = struct.unpack('>II', request[0:4+4])
        logging.info('volume{0} segment{1} stop schedule!'.format(volume_id, segment_id))
        self.stop_schedule_segment.add((volume_id, segment_id))

    def init_replayer(self, request, client_socket):
        reader = ConfigReader()
        replayer_num = int(reader.get('Replayers','replayer_num'))
        manager_ip = reader.get('Manager','ip')
        manager_port = int(reader.get('Manager','port'))
        replication = int(reader.get('Other Settings','replication'))
        self.replayer_2_segment = dict()
        replayer_2_volume = dict()
        volume_2_segments = dict()
        volumes = set()
        for volume_id, segment_id in self.segments:
            if volume_id not in volume_2_segments:
                volume_2_segments[volume_id] = list()
            volumes.add(volume_id)
            volume_2_segments[volume_id].append(segment_id)
        replayer_id = 0
        for volume_id in volumes:
            if replayer_id not in replayer_2_volume:
                replayer_2_volume[replayer_id]=list()
            replayer_2_volume[replayer_id].append(volume_id)
            replayer_id = (replayer_id + 1)%replayer_num
        for replayer_id in replayer_2_volume:
            for volume_id in replayer_2_volume[replayer_id]:
                for segment_id in volume_2_segments[volume_id]:
                    if replayer_id not in self.replayer_2_segment:
                        self.replayer_2_segment[replayer_id]=list()
                    self.replayer_2_segment[replayer_id].append((volume_id, segment_id))

        self.replayer_ip = dict()
        self.replayer_port = dict()
        self.ready_replayer_num = 0
        for i in range(0, replayer_num):
            # ip = reader.get('Replayer'+str(i), 'ip')
            ip = reader.get('Replayer'+str(0), 'ip')
            self.replayer_ip[i] = ip
        logging.info('Replayer ip')
        logging.info(self.replayer_ip)

        cmd_dict = dict()
        writing_mode = reader.get('Other Settings','writing_mode')
        buffer_writing_manner = reader.get('Other Settings','buffer_writing_manner')
        migration_gap = int(reader.get('Other Settings','migration_gap'))
        strip_length = int(reader.get('Other Settings','strip_length'))
        seg_num = int(reader.get('Other Settings','seg_num'))
        for i in range(0, replayer_num):
            replayer_id = i
            ip = self.replayer_ip[i]
            if ip not in cmd_dict:
                cmd_dict[ip] = list()
            cmd = 'ulimit -n 65535 && cd {0} && nohup sudo python3 Replayer.py -i {1} -m {2} -p {3} -s {4} -r {5} -w {6} -b {7} -l {8} -n {9} -o {10} -e {11} >> ../output/replayer_error 2> ../output/replayer_output{1} &'\
            .format(self.src, replayer_id, manager_ip, manager_port, self.segmentation, migration_gap, writing_mode, buffer_writing_manner, strip_length, seg_num, len(self.segments), replication)
            logging.info(cmd)
            cmd_dict[ip].append(cmd)
        for ip in cmd_dict:
            cmd_list = cmd_dict[ip]
            for i in range(0,int((len(cmd_list)+1-1)//1)):
                start = i*1
                end = min(len(cmd_list),(i+1)*1)
                self.start_processes(ip, cmd_list[start:end])

        while self.ready_replayer_num < replayer_num:
            logging.info('ready replayer number{0}'.format(self.ready_replayer_num))
            time.sleep(1)
        logging.info('all replayers set')
        # logging.info(self.replayer_ip)
        # logging.info(self.replayer_port)
        response = struct.pack('>B', SUCCEED)
        send(response, client_socket)

    def notify_replayer_ready(self, request, client_socket):
        replayer_id, replayer_port = struct.unpack('>II', request[0:8])
        self.replayer_port[replayer_id] = replayer_port
        self.ready_replayer_num = self.ready_replayer_num + 1

    def start_replayer(self, request, client_socket):
        reader = ConfigReader()
        replayer_num = len(self.replayer_ip)
        start_trace_time = float('inf')
        end_trace_time = -1
        trace_path = reader.get('Manager','trace_path')
        volume_start = int(reader.get('Volumes','start'))
        volume_end = int(reader.get('Volumes','end'))
        volume_list = list(range(volume_start, volume_end))
        for volume in volume_list:
            volume_trace_path = trace_path+'volume_'+str(volume)+'.csv'
            if not os.path.isfile(volume_trace_path):
                continue
            volume_trace = pd.read_csv(volume_trace_path)
            if len(volume_trace)==0:
                continue
            if volume_trace['time'][0] < start_trace_time:
                start_trace_time = volume_trace['time'][0]
            if volume_trace['time'][len(volume_trace)-1] > end_trace_time:
                end_trace_time = volume_trace['time'][len(volume_trace)-1]
        start_sys_time = time.time() + 10
        start_sys_time = start_sys_time*1000000
        localtime = time.asctime(time.localtime(start_sys_time/1000000))
        logging.info('starting time {0}'.format(localtime))
        tracetime = time.asctime(time.localtime(start_trace_time/1000000))
        logging.info('trace replaying starts: {0}'.format(tracetime))
        traceendtime = time.asctime(time.localtime(end_trace_time/1000000))
        logging.info('trace replaying ends: {0}'.format(traceendtime))

        start_sys_time = str(start_sys_time)
        start_trace_time = str(start_trace_time)
        end_trace_time = str(end_trace_time)
        start_sys_time_len = len(start_sys_time)
        start_trace_time_len = len(start_trace_time)
        end_trace_time_len = len(end_trace_time)
        for i in range(0, replayer_num):
            ip = self.replayer_ip[i]
            port = self.replayer_port[i]
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip,port))
            request = bytearray()
            request.extend(struct.pack('>B', START_REPLAY))
            request.extend(struct.pack('>I',start_sys_time_len))
            request.extend(bytearray(start_sys_time,encoding='utf-8'))
            request.extend(struct.pack('>I',start_trace_time_len))
            request.extend(bytearray(start_trace_time,encoding='utf-8'))
            request.extend(struct.pack('>I',end_trace_time_len))
            request.extend(bytearray(end_trace_time,encoding='utf-8'))
            send(request, s)
        logging.info('sending over!')

    def shutdown_manager(self, request, client_socket):
        sys.exit(0)

    def get_segment_info(self, request, client_socket):
        try:
            replayer_id = struct.unpack('>I', request[0:4])[0]
            response = bytearray()
            if replayer_id in self.replayer_2_segment:
                # logging.info(self.replayer_2_segment[replayer_id])
                for volume_id, segment_id in self.replayer_2_segment[replayer_id]:
                    # if segment_id != 0:
                    #     continue
                    response += struct.pack('>I',volume_id)
                    response += struct.pack('>I',segment_id)
                    response += struct.pack('>I',1)
                    ip = self.forwarding_server_ip[self.segment_pos[(volume_id, segment_id)]]
                    port = self.forwarding_proxy_port[(volume_id, segment_id)]
                    ip_len = len(ip)
                    response += struct.pack('>I',ip_len)
                    response += bytearray(ip, encoding='utf-8')
                    response += struct.pack('>I',port)
            send(response, client_socket)
        except Exception as err:
            logging.info(err)

    def get_replica_segment_info(self, request, client_socket):
        try:
            replayer_id = struct.unpack('>I', request[0:4])[0]
            response = bytearray()
            reader = ConfigReader()
            replica_num = int(reader.get('Servers','replica_num'))
            if replayer_id in self.replayer_2_segment:
                # logging.info(self.replayer_2_segment[replayer_id])
                for volume_id, segment_id in self.replayer_2_segment[replayer_id]:
                    # if segment_id != 0:
                    #     continue
                    response += struct.pack('>I',volume_id)
                    response += struct.pack('>I',segment_id)
                    response += struct.pack('>I',replica_num)
                    for i in range(0, replica_num):
                        ip = self.forwarding_server_ip[self.segment_pos[(volume_id, segment_id)][i]]
                        port = self.forwarding_proxy_port[(volume_id, segment_id, i)]
                        ip_len = len(ip)
                        response += struct.pack('>I',ip_len)
                        response += bytearray(ip, encoding='utf-8')
                        response += struct.pack('>I',port)
            send(response, client_socket)
        except Exception as err:
            logging.info(err)

    def get_server_segment_info(self, request, client_socket):
        response = bytearray()
        log_merger_id = struct.unpack('>I', request[0:4])[0]
        for volume_id, segment_id in self.segment_pos:
            if self.segment_pos[(volume_id, segment_id)] == log_merger_id:

                response += struct.pack('>I',volume_id)
                response += struct.pack('>I',segment_id)
                port = self.forwarding_proxy_port[(volume_id, segment_id)]
                response += struct.pack('>I',port)
        send(response, client_socket)

    def get_forwarding_proxy_info(self, request, client_socket):
        volume_id, segment_id = struct.unpack('>II', request[0:4+4])
        ip = self.forwarding_server_ip[self.segment_pos[(volume_id, segment_id)]]
        port = self.forwarding_proxy_port[(volume_id, segment_id)]
        ip_len = len(ip)
        response = struct.pack('>I',ip_len)
        response += bytearray(ip,encoding='utf-8')
        response += struct.pack('>I',port)
        send(response, client_socket)

    def get_buffer_proxy_info(self, request, client_socket):
        volume_id = struct.unpack('>I', request[0:4])[0]
        response = bytearray()
        for i in range(0, len(self.buffer_proxy_ip[volume_id])):
            ip = self.buffer_proxy_ip[volume_id][i]
            port = self.buffer_proxy_port[(volume_id, i)]
            ip_len = len(ip)
            response += struct.pack('>I',ip_len)
            response += bytearray(ip, encoding = 'utf-8')
            response += struct.pack('>I',port)
            # logging.info((ip, port))
        send(response, client_socket)
    
    def get_log_merger_port(self, request, client_socket):
        reader = ConfigReader()
        log_merger_port = int(reader.get('Log Merger','port'))
        response = struct.pack('>I',log_merger_port)
        send(response, client_socket)

    def get_trace_path(self, request, client_socket):
        reader = ConfigReader()
        trace_path = reader.get('Manager','trace_path')
        response = bytearray(trace_path, encoding='utf-8')
        send(response, client_socket)

    def get_log_path(self, request, client_socket):
        reader = ConfigReader()
        log_path = reader.get('Replayers','log_path')
        response = bytearray(log_path, encoding='utf-8')
        send(response, client_socket)

    def report_load(self, request, client_socket):
        volume_id, segment_id, load = struct.unpack('>III', request[0: 4+4+4])
        if self.alpha == -1:
            self.segment_load[(volume_id, segment_id)] = load
        else:
            self.segment_load[(volume_id, segment_id)] = self.alpha*load + (1-self.alpha)*self.segment_load[(volume_id, segment_id)]
        self.start_schedule = 1

    def migrate(self, volume_id, segment_id, target_server_id, segment_pos, forwarding_proxy_port):
        old_pos = segment_pos[(volume_id, segment_id)]
        old_port = forwarding_proxy_port[(volume_id, segment_id)]
        reader = ConfigReader()
        log_merger_port = int(reader.get('Log Merger','port'))

        forwarding_proxy_ip = self.forwarding_server_ip[old_pos]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((forwarding_proxy_ip,log_merger_port))
        request = struct.pack('>BII',REMOVE_SEGMENT, volume_id, segment_id)
        send(request, s)
        recv(s)
        s.close()
        logging.info('removing the original log merger manager, VDisk {0} segment{1}, pos {2} Succeeds!'.format(volume_id, segment_id, old_pos))
        
        forwarding_proxy_ip = self.forwarding_server_ip[old_pos]
        forwarding_proxy_port = old_port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((forwarding_proxy_ip, forwarding_proxy_port))
        request = struct.pack('>BII',SWITCH_ZOMBIE, volume_id, segment_id)
        send(request, s)
        response = recv(s)
        log_head = struct.unpack('>Q', response[0:8])[0]
        s.close()
        logging.info('switching old forwarding proxy {0} port {1} to zombie state'.format(forwarding_proxy_ip, forwarding_proxy_port))

        log_merger_ip = self.forwarding_server_ip[target_server_id]
        forwarding_porxy_port = self.forwarding_proxy_port[(volume_id, segment_id)]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((log_merger_ip, log_merger_port))
        request = struct.pack('>BIIIQ',ADD_SEGMENT, volume_id, segment_id, forwarding_porxy_port, log_head)
        send(request, s)
        recv(s)
        s.close()
        logging.info('creating new log merger at targeted server for VDisk {0} segment{1}'.format(volume_id, segment_id))

        self.migrate_num += 1

    def alg1(self, segment_load, segment_pos, server_num):
        server_segment_list = dict()
        for i in range(0, server_num):
            server_segment_list[i] = list()
        server_load = dict()
        for i in range(0, server_num):
            server_load[i] = 0
        avg_server_load = 0
        for volume_id, segment_id in segment_load.keys():
            server_segment_list[segment_pos[(volume_id, segment_id)]].append(((volume_id, segment_id), segment_load[(volume_id, segment_id)]))
            server_load[segment_pos[(volume_id, segment_id)]]+=segment_load[(volume_id, segment_id)]
            avg_server_load += segment_load[(volume_id, segment_id)]
        avg_server_load /= server_num


        for i in server_segment_list:
            server_segment_list[i].sort(key = lambda segment: segment[1], reverse = True)
        migration_plan = list()
        # logging.info(server_load)
        while True:
            max_server = max(server_load,key=server_load.get)
            min_server = min(server_load,key=server_load.get)
            # if server_load[max_server]/avg_server_load < 2:
            #     return list()
            # logging.info('max server {0}'.format(max_server))
            # logging.info('min server {0}'.format(min_server))
            if server_load[max_server] <= server_load[min_server]:
                del(server_load[max_server])
                break
            flag = 0
            for segment in server_segment_list[max_server]:
                if segment[1] + server_load[min_server] > avg_server_load or segment[1] + server_load[min_server] > server_load[max_server] - segment[1]:
                    continue
                if segment[1]/avg_server_load < 0.01:
                    del(server_load[max_server])
                    flag = 1
                    break
                migration_plan.append((segment[0], min_server))
                server_segment_list[max_server].remove(segment)
                server_segment_list[min_server].append(segment)
                server_load[max_server] -= segment[1]
                server_load[min_server] += segment[1]
                flag = 1
                break
            if flag == 0:
                del(server_load[max_server])
            if len(server_load) == 0:
                break
        return migration_plan

    def schedule(self):
        reader = ConfigReader()
        replication = int(reader.get('Other Settings','replication'))
        if replication != 0:
            logging.info('replication mode, cancel proxy schedule!')
            return
        while len(self.segment_pos) ==0:
            time.sleep(0.1)
        segments = self.segment_pos.keys()
        while self.start_schedule == 0:
            time.sleep(1)
        logging.info('migrating starts')
        migration_gap = int(reader.get('Other Settings','migration_gap'))
        if migration_gap == -1:
            return
        logging.info(migration_gap)
        time.sleep(migration_gap + 1)
        scheduling_alg = reader.get('Other Settings','scheduling_alg')
        while True:
            logging.info('migration strategy:#{0}#'.format(scheduling_alg))
            # logging.info(self.segment_load)
            segment_load = dict((key,value) for key,value in self.segment_load.items() if key not in self.stop_schedule_segment)
            if scheduling_alg == 'alg1':
                migration_plan = self.alg1(segment_load, self.segment_pos, len(self.forwarding_server_ip))
            else:
                logging.info('now only alg1 is availale')
            logging.info('current layout:')
            # logging.info(self.segment_pos)
            logging.info('migration plan:')
            logging.info(migration_plan)
            self.schedule_count+=len(migration_plan)
            
            start_time = time.time()*1000000
            segment_pos = dict(self.segment_pos)
            forwarding_proxy_port = dict(self.forwarding_proxy_port)
            target_server_list = list()
            segment_list = list()
            segment_dict = dict()
            for plan in migration_plan:
                volume_id,segment_id = plan[0]
                target_server_id = plan[1]
                segment_list.append((volume_id, segment_id))
                target_server_list.append(target_server_id)
                if target_server_id not in segment_dict:
                    segment_dict[target_server_id] = list()
                segment_dict[target_server_id].append((volume_id, segment_id))
            thread_list = list()
            for target_server_id in set(target_server_list):
                thread_list.append(gevent.spawn(self.create_forwarding_proxy, segment_dict[target_server_id], target_server_id))
            gevent.wait(thread_list)

            for volume_id, segment_id in segment_list:
                while self.forwarding_proxy_port[(volume_id, segment_id)] == -1:
                    time.sleep(0.001)
            
            self.migrate_num = 0
            migration_list = list()
            for plan in migration_plan:
                volume_id, segment_id = plan[0]
                target_server_id = plan[1]
                migration_list.append(gevent.spawn(self.migrate, volume_id, segment_id, target_server_id, segment_pos, forwarding_proxy_port))
            gevent.wait(migration_list)
            if self.migrate_num != len(migration_plan):
                logging.info('migration fails')
                sys.exit(0)
            else:
                logging.info('migration done')
            end_time = time.time()*1000000
            logging.info('this epoch of migration costs {0} us'.format(end_time - start_time))
            time.sleep(migration_gap)
            

    def report_stable_bandwidth(self, request, client_socket):
        volume_id, segment_id, bandwidth = struct.unpack('>III',request[0:4+4+4])
        server_id = self.segment_pos[(volume_id, segment_id)]
        if (volume_id,segment_id) in self.stable_bandwidth_dict:
            self.bandwidth_stable_used[server_id] -= self.stable_bandwidth_dict[(volume_id,segment_id)]
        self.stable_bandwidth_dict[(volume_id,segment_id)] = bandwidth
        self.bandwidth_stable_used[server_id] += bandwidth

    def get_free_bandwidth(self, request, client_socket):
        response = bytearray()
        for i in range(0, self.buffer_server_num):
            free_bandwidth = self.bandwidth_max - self.bandwidth_stable_used[i]
            response = response + struct.pack('>I',free_bandwidth)
        send(response, client_socket)

    def print_stable_bandwidth(self):
        while True:
            for i in range(0, self.buffer_server_num):
                free_bandwidth = self.bandwidth_max - self.bandwidth_stable_used[i]
            time.sleep(10)

    def reset_sys(self, request, client_socket):
        start = 0 
        req_len = len(request)
        src_node = struct.unpack('>I', request[0:4])[0]
        start += 4
        target_node_list = list()
        while start < req_len:
            target_node = struct.unpack('>I', request[start:start+4])[0]
            start += 4
            target_node_list.append(target_node)
        logging.info(src_node)
        logging.info(target_node_list)

        ip = '10.0.0.5' + str(src_node)
        cmd = 'cd /home/k8s/wzhzhu && tar cvf DiffForward_src.tar DiffForward/src'
        self.start_process2(ip, cmd)
        logging.info('source code packaging success！')

        for node in target_node_list:
            ip = '10.0.0.5' + str(node)
            if ip == '10.0.0.52':
                logging.info('modifying source node is not permitted')
                sys.exit(0)
            cmd = 'cd /home/k8s/wzhzhu && sudo rm -rf DiffForward/src && scp k8s@node{0}:/home/k8s/wzhzhu/DiffForward_src.tar . && tar xvf DiffForward_src.tar -C /home/k8s/wzhzhu'\
                .format(src_node)
            self.start_process2(ip, cmd)
        logging.info('source code updating success！')

        for node in target_node_list:
            ip = '10.0.0.5' + str(node)
            if ip == '10.0.0.52':
                logging.info('modifying source node is not permitted')
                sys.exit(0)
            # cmd = 'cd /home/k8s/wzhzhu/ && sudo rm -rf volume_data && mkdir volume_data'
            # self.start_process2(ip, cmd)
            cmd = 'cd /home/k8s/wzhzhu/buffer_space && ls | xargs -n 10 rm'
            self.start_process2(ip, cmd)
            cmd = 'cd /home/k8s/wzhzhu/DiffForward/output && ls | xargs -n 10 rm'
            self.start_process2(ip, cmd)
        logging.info('clear space！')
        response = bytearray()
        send(response, client_socket)

    def restrict_bandwidth(self, request, client_socket):
        bandwidth_max = struct.unpack('>I', request[0:4])[0]
        logging.info(bandwidth_max)

        bandwidth_max = bandwidth_max * 4 * 8
        nic_name = dict()
        nic_name[1] = 'enp59s0f1'
        nic_name[2] = 'ens9'
        nic_name[3] = 'ens9'
        nic_name[4] = 'enp94s0f1'
        nic_name[5] = 'enp59s0f1'
        nic_name[6] = 'enp59s0f1'
        nic_name[7] = 'enp5s0f1'

        for node in [1,3,4,5,6]:
            ip = '10.0.0.5' + str(node)
            cmd = 'sudo wondershaper -a {0} -d {1} -u {2}'.format(nic_name[node], bandwidth_max, bandwidth_max)
            self.start_process2(ip, cmd)
        response = bytearray()
        send(response, client_socket)

    def unrestrict_bandwidth(self, request, client_socket):
        nic_name = dict()
        nic_name[1] = 'enp59s0f1'
        nic_name[2] = 'ens9'
        nic_name[3] = 'ens9'
        nic_name[4] = 'enp94s0f1'
        nic_name[5] = 'enp59s0f1'
        nic_name[6] = 'enp59s0f1'
        nic_name[7] = 'enp5s0f1'
        for node in [1,3,4,5,6]:
            ip = '10.0.0.5' + str(node)
            cmd = 'sudo wondershaper -c -a {0}'.format(nic_name[node])
            self.start_process2(ip, cmd)
        response = bytearray()
        send(response, client_socket)

    def clean_sys(self, request, client_socket):
        for node in [1,3,4,5,6,2]:
            ip = '10.0.0.5' + str(node)
            cmd = 'cd /home/k8s/wzhzhu && sudo bash kill'
            self.start_process2(ip, cmd)
        for node in [1,3,4,5,6]:
            ip = '10.0.0.5' + str(node)
            if ip == '10.0.0.52':
                logging.info('modifying source node is not permitted')
                sys.exit(0)
            cmd = 'cd /home/k8s/wzhzhu/buffer_space && ls | xargs -n 10 rm'
            self.start_process2(ip, cmd)
            cmd = 'cd /home/k8s/wzhzhu/DiffForward/output && ls | xargs -n 10 rm'
            self.start_process2(ip, cmd)
        response = bytearray()
        send(response, client_socket)




def main(argv):
    # p.nice(-20)
    manager = Manager()
    manager.run()

if __name__ == "__main__":
    main(sys.argv[1:])


