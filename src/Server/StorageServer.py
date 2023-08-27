#!/usr/bin/python2
#coding:utf-8
from gevent import monkey;monkey.patch_all()
import sys
import gevent
from gevent.event import Event
import socket
from ..Properties.config import *
import time
import getopt
from ..Utils.DataTrans import *
import struct
import psutil, os
from heapq import *
import collections
import math
import logging
import rados
import threading
import signal
import gc
gc.disable()

p = psutil.Process(os.getpid())

def on_complete(completion, data_read):
    return

class StorageServer:
    def __init__(self, manager_ip, manager_port, storage_server_id):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        domanin_socket = True
        # domain socket
        if domanin_socket == True:
            self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            serverAddr = '/home/k8s/wzhzhu/uds'+str(storage_server_id)
            if os.path.exists(serverAddr):
                os.unlink(serverAddr)
            self.server_socket.bind(serverAddr)
            self.port = 1
        else:
            # network socket     
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
            self.server_socket.bind(('0.0.0.0', 0))
            self.port = self.server_socket.getsockname()[1]   
        #禁用nagle算法
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.listen(100000000)
        #初始化
        self.manager_ip = manager_ip
        self.manager_port = int(manager_port)
        self.storage_server_id = int(storage_server_id)
        self.set_manager(manager_ip, manager_port)
        self.data_seg = bytearray('8'*1024*1024, encoding='utf-8')
        #连接存储层ceph
        self.cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        self.mount_storage()

        #读写请求队列
        self.write_temp_queue = collections.deque()
        self.read_temp_queue = collections.deque()
        self.write_pending_queue = collections.deque()
        self.read_pending_queue = collections.deque()
        #读写请求完成队列
        self.write_finished_queues = dict()
        self.read_finished_queues = dict()
        #日志队列
        self.merge_queue = collections.deque()
        self.merge_waiting_queue = collections.deque()
        self.merge_log_tail = dict()
        self.inflight_merging_traffic = 0

        '''Events'''
        #提交请求通知
        self.submit_event = Event()
        #处理请求通知
        self.process_read_event = Event()
        self.process_write_event = Event()
        #标志暂停还是继续返回执行完毕的请求信息给client
        self.get_finished_write_events = dict()
        self.get_finished_read_events = dict()
        self.segment_valid = dict()
        #merge处理通知
        self.merge_submit_event = Event()
        self.process_merge_event = Event()

        self.pass_time = False
        # self.pass_time = True
        '''Threads'''
        #处理读写请求的线程
        self.process_write_thread = gevent.spawn(self.process_write)
        self.process_read_thread = gevent.spawn(self.process_read)
        self.submit_thread = gevent.spawn(self.submit)
        self.merge_submit_thread = gevent.spawn(self.merge_submit)
        self.process_merge_thread = gevent.spawn(self.process_merge)
        self.detect_thread = gevent.spawn(self.detect_dead)

        self.thread_stuck = dict()
        self.thread_stuck['submit'] = False
        self.thread_stuck['merge_submit'] = False
        self.merge_finish_dict = dict()
        self.merge_log_head = dict()
        self.notify_log_head_event = dict()

        # msc reporter
        self.init_merge_speed_controller_socket()
        self.queue_state = Event()
        self.report_msc_event = Event()
        self.queue_num_read = self.queue_num_write = 0
        # 向merge speed controller实时汇报队列状态
        gevent.spawn(self.report_msc)
        #回传req server的port
        self.send_port()

    def init_merge_speed_controller_socket(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/home/k8s/wzhzhu/merge_ctrl_uds')
        self.mscs_send  = s

    '''设定本manager的ip和port'''
    def set_manager(self, manager_ip, manager_port):
        #连接manager
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.manager_socket.connect((manager_ip, manager_port))

    '''准备工作做好之后通知manager自己负责的虚拟盘，以及port[NOTIFY_PORT,ip_len,ip,port]'''
    def send_port(self):
        s = self.manager_socket
        request = struct.pack('>B',NOTIFY_STORAGE_SERVER_ID)
        request += struct.pack('>I', self.storage_server_id)
        request += struct.pack('>I', self.port)
        send(request, s)
        s.close()

    def run(self):
        self.serve()

    def serve(self):
        while True:
            client_socket, client_addr = self.server_socket.accept()
            logging.info(('connection', client_addr))
            gevent.spawn(self.recv_request, client_socket, client_addr)

    def recv_request(self, client_socket, client_addr):
        try:
            while True:
                op = -1
                request = recv(client_socket)
                if not request:
                    logging.info("{0} offline".format(client_addr))
                    client_socket.close()
                    break
                op = struct.unpack('>B', request[0:1])[0]
                if op == STORAGE_READ:
                    self.read(request[1:], client_socket)
                elif op == STORAGE_WRITE:
                    self.write(request[1:], client_socket)
                elif op == STORAGE_GET_FINISHED_WRITE:
                    self.get_finished_write(request[1:], client_socket)
                elif op == STORAGE_GET_FINISHED_READ:
                    self.get_finished_read(request[1:], client_socket)
                elif op == REGISTER_SEGMENT:
                    self.register_segment(request[1:], client_socket)
                elif op == MERGE_LOG:
                    self.merge_log(request[1:], client_socket)
                elif op == UNREGISTER_SEGMENT:
                    self.unregister_segment(request[1:], client_socket)
                elif op == GET_MERGE_LOG_HEAD:
                    self.get_merge_log_head(request[1:], client_socket)
                elif op == START_SUBSCRIBE_MSC:
                    self.start_subscribe_msc(request[1:], client_socket)
                elif op == END_SUBSCRIBE_MSC:
                    self.end_subscribe_msc(request[1:], client_socket)
                else:
                    logging.info('Something wrong!')
                    sys.exit(0)
        except Exception as err:
            logging.info("recv_data error1:{0}".format(err))
        finally:
            logging.info('socket close!, addr = {0}, op = {1}'.format(client_addr, op))
            client_socket.close()
            return 1

    #挂载存储层客户端
    def mount_storage(self):
        self.cluster.connect()
        self.ioctx = self.cluster.open_ioctx('storage-layer')
        logging.info('create rados ctx succeed!')

    #解挂存储层客户端
    def unmount_storage(self):
        self.ioctx.close()
        logging.info('ioctx close succeed!')
        self.cluster.shutdown()
        logging.info('shutdown rados ctx succeed!')

    def start_subscribe_msc(self, request, client_socket):
        total_log_size, predict_time, log_num = struct.unpack('>III', request[0:4+4+4])
        self.total_log_size = total_log_size
        self.log_gap = 0.001*predict_time/log_num
        self.pull_gap_per_byte = 10/(self.total_log_size*1024*1024)
        self.mscs_recv = None
        self.subscribe_thread = gevent.spawn(self.subscribe_msc_out)

    def end_subscribe_msc(self, request, client_socket):
        self.subscribe_thread.kill()
        if self.mscs_recv is not None:
            self.mscs_recv.close()
        self.queue_state.clear()
        self.pass_time = False
        response = bytearray()
        send(response, client_socket)

    def register_segment(self, request, client_socket):
        volume_id, segment_id = struct.unpack('>II',request[0:0+4+4])
        logging.info('volume{0} segment{1} register！'.format(volume_id, segment_id))
        self.write_finished_queues[(volume_id, segment_id)] = collections.deque()
        self.read_finished_queues[(volume_id, segment_id)] = collections.deque()
        self.get_finished_write_events[(volume_id, segment_id)] = Event()
        self.get_finished_read_events[(volume_id, segment_id)] = Event()
        self.segment_valid[(volume_id, segment_id)] = True
        self.notify_log_head_event[(volume_id, segment_id)] = Event()
        request = bytearray()
        send(request, client_socket)

    def unregister_segment(self, request, client_socket):
        volume_id, segment_id = struct.unpack('>II',request[0:0+4+4])
        logging.info('volume{0} segment{1} clear！'.format(volume_id, segment_id))
        del self.write_finished_queues[(volume_id, segment_id)]
        del self.read_finished_queues[(volume_id, segment_id)]
        self.segment_valid[(volume_id, segment_id)] = False
        self.get_finished_write_events[(volume_id, segment_id)].set()
        self.get_finished_read_events[(volume_id, segment_id)].set()
        self.notify_log_head_event[(volume_id, segment_id)].set()
        del self.notify_log_head_event[(volume_id, segment_id)]
        request = bytearray()
        send(request, client_socket)

    def merge_log(self, request, client_socket):
        start = 0
        request_len = len(request)
        while start < request_len:
            volume_id, segment_id, offset, length, log_id, total_log_size = struct.unpack('>IIQIQI', request[start: start + 4 + 4 + 8 + 4 + 8 + 4])
            start += 4 + 4 + 8 + 4 + 8 + 4
            data = request[start: start + length]
            start += length
            gap = self.pull_gap_per_byte*length
            self.merge_queue.append((volume_id, segment_id, offset, length, log_id, data, gap))
        self.merge_submit_event.set()

    def merge_submit(self):
        try:
            while True:
                self.merge_submit_event.wait()
                while len(self.merge_queue) != 0:
                    if self.inflight_merging_traffic > 1024*1024:
                        time.sleep(0.001)
                        continue
                    if not self.pass_time:
                        self.queue_state.wait()
                    self.thread_stuck['merge_submit'] = True
                    volume_id, segment_id, offset, length, log_id, data, gap = self.merge_queue.popleft()
                    self.merge_queue.appendleft((volume_id, segment_id, offset, length, log_id, data, gap))
                    object_id = str(volume_id) + '_' + str(int(offset/(64*1024)))
                    object_offset = offset%(64*1024)
                    completion = self.ioctx.aio_write(object_id, str(data), object_offset)
                    self.merge_queue.popleft()
                    self.inflight_merging_traffic += length
                    self.merge_waiting_queue.append((completion, volume_id, segment_id, log_id, length))
                    self.process_merge_event.set()
                    self.thread_stuck['merge_submit'] = False
                    # time.sleep(self.log_gap)
                    time.sleep(gap)
                    gevent.idle(0)
                self.merge_submit_event.clear()
        except Exception as err:
            logging.info(err)

    '''查询合并完成的日志，并通知已完成的合并任务'''
    def process_merge(self):
        try:
            while True:
                self.process_merge_event.wait()
                while len(self.merge_waiting_queue) != 0:
                    completion, volume_id, segment_id, log_id, length = self.merge_waiting_queue.popleft()
                    if completion.is_safe:
                        self.merge_log_head[(volume_id, segment_id)] = log_id
                        self.inflight_merging_traffic -= length
                        if len(self.merge_waiting_queue) == 0:
                            self.notify_log_head_event[(volume_id, segment_id)].set()
                    else:
                        self.merge_waiting_queue.appendleft((completion, volume_id, segment_id, log_id, length))
                        time.sleep(0.001)
                self.process_merge_event.clear()
        except Exception as err:
            logging.info(err)

    def get_merge_log_head(self, request, client_socket):
        try:
            volume_id, segment_id = struct.unpack('>II',request[0:4+4])
            while True:
                self.notify_log_head_event[(volume_id, segment_id)].wait()
                if (volume_id,segment_id) not in self.notify_log_head_event:
                    logging.info('stop get merge log head')
                    return
                merge_log_head = self.merge_log_head[(volume_id, segment_id)]
                request = struct.pack('>Q',merge_log_head)
                send(request, client_socket)
                self.notify_log_head_event[(volume_id, segment_id)].clear()
        except Exception as err:
            logging.info(err)

    def write(self, request, client_socket):
        start = 0
        request_len = len(request)
        while start < request_len:
            req_id, sub_req_id, volume_id, segment_id, offset, length = struct.unpack('>QIIIQI', request[start: start + 8 + 4 + 4 + 4 + 8 + 4])
            start += 8 + 4 + 4 + 4 + 8 + 4
            data = request[start: start + length]
            start += length
            self.write_temp_queue.append((req_id, sub_req_id, volume_id, segment_id, offset, length, data))
        self.submit_event.set()

    def read(self, request, client_socket):
        start = 0
        request_len = len(request)
        while start < request_len:
            req_id, sub_req_id, volume_id, segment_id, offset, length = struct.unpack('>QIIIQI', request[start: start + 8 + 4 + 4 + 4 + 8 + 4])
            start += 8 + 4 + 4 + 4 + 8 + 4
            self.read_temp_queue.append((req_id, sub_req_id, volume_id, segment_id, offset, length))
        self.submit_event.set()

    def submit(self):
        while True:
            self.submit_event.wait()
            # gevent.idle(0)
            self.queue_num_write = len(self.write_pending_queue) + len(self.write_temp_queue)
            self.queue_num_read = len(self.read_pending_queue) + len(self.read_temp_queue)
            self.report_msc_event.set()
            time.sleep(0)
            self.thread_stuck['submit'] = True
            while len(self.write_temp_queue)!=0:
                try:
                    element = self.write_temp_queue.popleft()
                    req_id, sub_req_id, volume_id, segment_id, offset, length, data = element
                    self.write_temp_queue.appendleft(element)
                    object_id = str(volume_id) + '_' + str(int(offset/(64*1024)))
                    object_offset = offset%(64*1024)
                    completion = self.ioctx.aio_write(object_id, str(data), object_offset)
                    if completion is None:
                        logging.info('something went wrong')
                    self.write_temp_queue.popleft()
                    self.write_pending_queue.append((req_id, sub_req_id, volume_id, segment_id, offset, length, completion))
                    self.process_write_event.set()
                except Exception as err:
                    logging.info('Something went wrong!')
                    logging.info("write_data error:{0}".format(err))

            while len(self.read_temp_queue)!=0:
                try:
                    element = self.read_temp_queue.popleft()
                    req_id, sub_req_id, volume_id, segment_id, offset, length = element
                    self.read_temp_queue.appendleft(element)
                    object_id = str(volume_id) + '_' + str(int(offset/(64*1024)))
                    object_offset = offset%(64*1024)
                    completion = self.ioctx.aio_read(object_id, length, object_offset, oncomplete=on_complete)
                    if completion is None:
                        logging.info('something went wrong')
                    self.read_temp_queue.popleft()
                    self.read_pending_queue.append((req_id, sub_req_id, volume_id, segment_id, offset, length, completion))
                    self.process_read_event.set()
                except Exception as err:
                    logging.info('Something went wrong!')
                    logging.info("read_data error:{0}".format(err))

            if len(self.write_temp_queue) == 0 and len(self.read_temp_queue) == 0:
                self.submit_event.clear()
            self.thread_stuck['submit'] = False

    def process_write(self):
        while True:
            self.process_write_event.wait()
            while len(self.write_pending_queue) != 0:
                element = self.write_pending_queue.popleft()
                req_id, sub_req_id, volume_id, segment_id, offset, length, completion = element
                if completion.is_safe:
                    state = SUCCEED
                    self.write_finished_queues[(volume_id, segment_id)].append((req_id, sub_req_id, offset, length, state))
                    self.get_finished_write_events[(volume_id, segment_id)].set()
                else:
                    self.write_pending_queue.appendleft(element)
                    gevent.idle(0)
                    # time.sleep(0.001)
            self.process_write_event.clear()
            self.queue_num_write = len(self.write_pending_queue) + len(self.write_temp_queue)
            self.report_msc_event.set()

    def process_read(self):
        while True:
            self.process_read_event.wait()
            while len(self.read_pending_queue) != 0:
                element = self.read_pending_queue.popleft()
                req_id, sub_req_id, volume_id, segment_id, offset, length, completion = element
                if completion.is_safe:
                    """python interface of ceph rados seems do not support placing retrieved data
                    of aysnc reads to specified location upon completion, so here we replace the result
                    with a a random string when we perceive the completion of async reads by its 'completion'
                    field. This method guarantees the same latency but compromises correctness, so it can only
                    serve as a performance simulation and must be solved in production environment.
                    """
                    self.read_finished_queues[(volume_id, segment_id)].append((req_id, sub_req_id, offset, self.data_seg[0:length], SUCCEED))
                    self.get_finished_read_events[(volume_id, segment_id)].set()
                else:
                    self.read_pending_queue.appendleft(element)
                    gevent.idle(0)
                    # time.sleep(0.001)
            self.process_read_event.clear()
            self.queue_num_read = len(self.read_pending_queue) + len(self.read_temp_queue)
            self.report_msc_event.set()

    '''periodically sense the state of threads'''
    def detect_dead(self):
        while True:
            if self.thread_stuck['submit'] == True:
                logging.info('restart submitting thread')
                self.submit_thread.kill()
                self.submit_thread = gevent.spawn(self.submit)
                self.thread_stuck['submit'] = False
            if self.thread_stuck['merge_submit'] == True:
                logging.info('resubmit merging thread')
                self.merge_submit_thread.kill()
                self.merge_submit_thread = gevent.spawn(self.merge_submit)
                self.thread_stuck['merge_submit'] = False
            time.sleep(0.01)

    def get_finished_write(self, request, client_socket):
        volume_id, segment_id = struct.unpack('>II',request[0:0+4+4])
        while True:
            self.get_finished_write_events[(volume_id, segment_id)].wait()
            if self.segment_valid[(volume_id, segment_id)] == False:
                del self.get_finished_write_events[(volume_id, segment_id)]
                return
            write_finished_queue = self.write_finished_queues[(volume_id, segment_id)]
            while len(write_finished_queue)!=0:
                response = bytearray()
                req_id, sub_req_id, offset, length, state = write_finished_queue.popleft()
                response.extend(struct.pack('>QIQIB', req_id, sub_req_id, offset, length, state))
                send(response, client_socket)
            if len(write_finished_queue) == 0:
                self.get_finished_write_events[(volume_id, segment_id)].clear()

    def get_finished_read(self, request, client_socket):
        volume_id, segment_id = struct.unpack('>II',request[0:0+4+4])
        while True:
            self.get_finished_read_events[(volume_id, segment_id)].wait()
            if self.segment_valid[(volume_id, segment_id)] == False:
                del self.get_finished_read_events[(volume_id, segment_id)]
                return
            read_finished_queue = self.read_finished_queues[(volume_id, segment_id)]
            while len(read_finished_queue)!=0:
                response = bytearray()
                req_id, sub_req_id, offset, data, state = read_finished_queue.popleft()
                response.extend(struct.pack('>QIQIB', req_id, sub_req_id, offset, len(data), state))
                if state == SUCCEED:
                    response.extend(bytearray(data))
                send(response, client_socket)
            if len(read_finished_queue)==0:
                self.get_finished_read_events[(volume_id, segment_id)].clear()

    def subscribe_msc_out(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/home/k8s/wzhzhu/merge_ctrl_uds')
        self.mscs_recv  = s
        request = struct.pack('>B',GET_OUT)
        send(request, self.mscs_recv)
        while True:
            response = recv(self.mscs_recv)
            queue_state = struct.unpack('>B',response[0:1])[0]
            if queue_state == SUCCEED:
                self.queue_state.set()
                # logging.info('msc switch on')
            else:
                self.queue_state.clear()
                # logging.info('msc switch off')
    
    def timer(self, target_time):
        time.sleep(target_time)
        self.pass_time = True

    def report_msc(self):
        try:
            last_queue_num = 0
            while True:
                self.report_msc_event.wait()
                self.report_msc_event.clear()
                queue_num = self.queue_num_write + self.queue_num_read
                if (last_queue_num == 0 and queue_num !=0) or (last_queue_num != 0 and queue_num ==0):
                    last_queue_num = queue_num
                    request = struct.pack('>BIQ',REPORT_STORAGE, self.storage_server_id, queue_num)
                    send(request, self.mscs_send)
        except Exception as err:
            logging.info(err)

def main(argv):
    # p.nice(-20)
    manager_ip = manager_port = storage_server_id = None
    opts, args = getopt.getopt(argv,"hi:p:s:",["manager_ip=","manager_port=","storage_server_id="])
    for opt, arg in opts:
        if opt == '-h':
            logging.info('StorageServer.py -i <manager_ip> -p <manager_port> -s <storage_server_id>')
            sys.exit(0)
        elif opt in ("-i", "--manager_ip"):
            manager_ip = arg
        elif opt in ("-p", "--manager_port"):
            manager_port = int(arg)
        elif opt in ("-s", "--storage_server_id"):
            storage_server_id = int(arg)

    bind_cpu_core = False
    if bind_cpu_core:
        p = psutil.Process(os.getpid())
        count = psutil.cpu_count()
        cpu_list = p.cpu_affinity()
        target_cpu_core = cpu_list[storage_server_id%len(cpu_list)]
        p.cpu_affinity([target_cpu_core])

    my_storage_server = StorageServer(manager_ip = manager_ip, manager_port = manager_port, storage_server_id = storage_server_id)
    my_storage_server.run()

if __name__ == "__main__":
    main(sys.argv[1:])



