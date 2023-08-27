#!/usr/bin/python2
#coding:utf-8
import time
import math
import collections

class BurstDetector:
    def __init__(self, volume_id):
        self.volume_id = volume_id
        self.queue_length = 0
        self.last_time = 0
        self.speed = 0
        self.peak_size = 0
        self.latency = 50000
        self.capacity = (self.speed * self.latency) / 1000000
        self.peak_traffic = 0
        self.normal_flag = 0
        self.normal_speed = 0
        self.normal_traffic = 0
        self.state = 0
        self.normal_sync_time = 0
        self.start_flag = 0
        self.history = collections.deque()

    def update_speed(self, speed):
        self.speed = speed

    def sync_queue_state(self, timestamp):
        current_time = timestamp
        last_time = self.last_time
        self.last_time = current_time
        self.queue_length = self.queue_length - (self.speed * (current_time - last_time)) / 1000000
        if self.queue_length < 0:
            self.queue_length = 0

    def update_normal_speed(self, timestamp, length):
        if self.normal_sync_time == 0:
            self.normal_sync_time = timestamp
        if timestamp - self.normal_sync_time >= 5*1000000:
            normal_speed = int((self.normal_traffic * 1000000) / (timestamp - self.normal_sync_time))
            self.normal_speed = 0.5*normal_speed + 0.5*self.normal_speed
            self.update_speed(self.normal_speed)
            self.sync_queue_state(timestamp)
            self.capacity = (16 * self.normal_speed * self.latency) / 1000000
            self.normal_sync_time = timestamp
            self.normal_traffic = 0
        self.normal_traffic += length // 4096

    def get_state(self):
        return self.state

    def get_normal_speed(self):
        return self.normal_speed
    
    def get_peak_size(self):
        return self.peak_size

    def put_wreq(self, offset, length, timestamp):
        current_time = timestamp
        self.update_normal_speed(current_time, length)
        last_time = self.last_time
        self.last_time = current_time
        if self.start_flag == 0:
            self.start_flag = 1
            self.queue_length = 0
            return 0
        elif self.start_flag == 1:
            self.queue_length = self.queue_length - (self.speed * (current_time - last_time)) / 1000000
        if self.queue_length < 0:
            self.queue_length = 0

        weight = 1
        for h in self.history:
            if abs( h[0] - offset) <= 128*1024:
                weight = 0.5
                break
        if len(self.history) >= 32:
            self.history.popleft()
        self.history.append((offset, length))
        if self.queue_length + weight*length // 4096 > self.capacity:
            state = 1
        else:
            state = 0
            self.queue_length += weight * length // 4096
        return state