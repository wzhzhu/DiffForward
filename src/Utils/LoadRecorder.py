#!/usr/bin/python2
#coding:utf-8
import gevent
import socket
import time
import struct
from ..Properties.config import *
from DataTrans import *
import math
import collections
import numpy as np
import logging
import random
from gevent.event import Event

class LoadRecorder:
    def __init__(self, volume_id, segment_id, manager_ip, manager_port, report_gap):
        logging.basicConfig(level=logging.INFO,#控制台打印的日志级别
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    #日志格式
                    )
        self.volume_id = volume_id
        self.segment_id = segment_id
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.set_manager()
        #调度需要，计算历史一段时间的流量均值
        self.report_gap = report_gap
        self.traffic_history = collections.deque()
        self.avg_traffic = 0
        self.predict_speed = -1
        self.report_event = Event()
        #汇报稳定带宽需要，目前用平滑后的流量均值
        self.stable_traffic = 0
        self.stable_speed = 0
    
    '''设定本replayer manager'''
    def set_manager(self):
        #连接manager
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.manager_socket.connect((self.manager_ip,self.manager_port))

    '''设置self.avg_sync_time,开始统计'''
    def start(self, timestamp):
        #上次清空avg负载时间
        self.avg_sync_time = timestamp
        self.stable_sync_time = timestamp
        #负载信息的轮次
        # gevent.spawn(self.report_stable_bandwidth_loop)
        #周期性汇报负载线程
        if self.report_gap == -1:
            return
        gevent.spawn(self.report_load_loop1)
        gevent.spawn(self.report_load_loop2)
    '''**********************************************************************************************
        将负载统计数据发送给manager同步，用于调度
    **********************************************************************************************'''
    def record_req(self, length):
        self.avg_traffic += int(length / 4096)

    def report_load_loop1(self):
         # 周期同步，目前1s
        while True:
            time.sleep(1)
            #统计当前avg speed
            self.update_avg_speed()
    
    def report_load_loop2(self):
         # 周期同步，目前1s
        while True:
            self.report_event.wait()
            self.report_event.clear()
            time.sleep(1*random.random())
            #发送给manager
            self.send_load()

    def update_avg_speed(self):
        timestamp = time.time()*1000000
        avg_speed = int((self.avg_traffic * 1000000) / (timestamp - self.avg_sync_time))
        self.traffic_history.append(avg_speed)
        self.avg_sync_time = timestamp
        self.avg_traffic = 0
        if len(self.traffic_history)>=self.report_gap:
            # self.traffic_history.popleft()
            self.predict_speed = int(np.mean(self.traffic_history))
            self.report_event.set()
            while len(self.traffic_history) > 0:
                self.traffic_history.popleft()

    def send_load(self):
        request = struct.pack('>BIII',REPORT_LOAD, self.volume_id, self.segment_id, self.predict_speed)
        send(request, self.manager_socket)
        # logging.info('汇报虚拟盘{0} segment{1} 负载{2}'.format(self.volume_id, self.segment_id, self.predict_speed))

    '''**********************************************************************************************
        将负载统计数据发送给manager同步，用于记录稳定带宽占用
    **********************************************************************************************'''
    def record_stable_bandwidth(self, length):
        self.stable_traffic += int(length / 4096)

    def report_stable_bandwidth_loop(self):
         # 周期同步，目前5s
        self.stable_bandwidth_usage_flag = 0
        while True:
            time.sleep(1)
            #统计当前avg speed
            self.update_and_send_stable_bandwidth()

    def update_and_send_stable_bandwidth(self):
        timestamp = time.time()*1000000
        if self.stable_sync_time == 0:
            self.stable_sync_time = timestamp
        if timestamp - self.stable_sync_time > 5*1000000:
            stable_bandwidth_usage = int((self.stable_traffic * 1000000) / (timestamp - self.stable_sync_time))
            if self.stable_bandwidth_usage_flag == 0:
                self.stable_bandwidth_usage = stable_bandwidth_usage
                self.stable_bandwidth_usage_flag = 1
            else:
                self.stable_bandwidth_usage = 0.5*self.stable_bandwidth_usage + 0.5*stable_bandwidth_usage
            self.stable_sync_time = timestamp
            self.stable_traffic = 0
            self.send_stable_bandwidth()

    def send_stable_bandwidth(self):
        # logging.info('汇报虚拟盘{0} segment{1} 负载{2}'.format(self.volume_id, self.segment_id, int(self.stable_bandwidth_usage)))
        request = struct.pack('>BIII',REPORT_STABLE_BANDWIDTH, self.volume_id, self.segment_id, int(self.stable_bandwidth_usage))
        send(request, self.manager_socket)