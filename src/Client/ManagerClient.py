#!/usr/bin/python2
#coding:utf-8
from ..Properties.config import *
from ..Utils.DataTrans import *
import struct
from ..Utils.ConfigReader import ConfigReader
import socket
import sys
import time
import shutil
import os
import paramiko
from ..Utils.mail import *

class ManagerClient:
    def __init__(self):
        self.start_manager()

    def start_manager(self):
        reader = ConfigReader()
        manager_ip = reader.get('Manager','ip')
        manager_port = int(reader.get('Manager','port'))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((manager_ip,manager_port))
        self.manager_socket = s

    def init_merge_speed_controller(self):
        request = struct.pack('>B',INIT_MERGE_SPEED_CONTROLLER)
        send(request, self.manager_socket)
        recv(self.manager_socket)

    def init_storage_server(self):
        request = struct.pack('>B',INIT_STORAGE_SERVER)
        send(request, self.manager_socket)
        recv(self.manager_socket)

    def init_buffer_process(self):
        request = struct.pack('>B',INIT_BUFFER_PROCESS)
        send(request, self.manager_socket)
        recv(self.manager_socket)

    def init_forwarding_process(self):
        request = struct.pack('>B',INIT_FORWARDING_PROCESS)
        send(request, self.manager_socket)
        recv(self.manager_socket)

    def init_log_merger(self):
        request = struct.pack('>B',INIT_LOG_MERGER)
        send(request, self.manager_socket)
        recv(self.manager_socket)

    def init_replayer(self):
        request = struct.pack('>B',INIT_REPLAYER)
        send(request, self.manager_socket)
        recv(self.manager_socket)

    def start_replayer(self):
        request = struct.pack('>B',START_REPLAYER)
        send(request, self.manager_socket)
     
    def reset_sys(self):
        src_node = 2
        target_node_list = [1,3,4,5,6]
        request = struct.pack('>BI', RESET_SYS, src_node)
        for node in target_node_list:
            request += struct.pack('>I', node)
        send(request ,self.manager_socket) 
        recv(self.manager_socket)

    def restrict_bandwidth(self,bandwidth_max):
        request = struct.pack('>BI', RESTRICT_BANDWIDTH, bandwidth_max)
        send(request ,self.manager_socket)
        recv(self.manager_socket)

    def unrestrict_bandwidth(self):
        request = struct.pack('>B', UNRESTRICT_BANDWIDTH)
        send(request ,self.manager_socket)
        recv(self.manager_socket)

    def clean_sys(self):
        request = struct.pack('>B', CLEAN_SYS)
        send(request ,self.manager_socket)
        recv(self.manager_socket)

    def shutdown_manager(self):
        request = struct.pack('>B', SHUTDOWN_MANAGER)
        send(request ,self.manager_socket)

    # def migrate(self, volume_id, segment_id, target_server_id):
    #     request = struct.pack('>BIII', MIGRATE, volume_id, segment_id, target_server_id)
    #     send(request ,self.manager_socket)

    def search_string(self, str, file):
        with open(file,'r') as foo:
            for line in foo.readlines():
                if str in line:
                    return True
        return False
    
    def get_schedule_count(self):
        request = struct.pack('>B', GET_SCHEDULE_COUNT)
        send(request ,self.manager_socket)
        response = recv(self.manager_socket)
        schedule_count = struct.unpack('>I',response[0:4])[0]
        return schedule_count

def copy_exp_result(exp_name):
    #create dir to record experimental results
    new_dir = "/home/k8s/wzhzhu/exp_data3/"+exp_name
    #mv results
    shutil.copytree('/home/k8s/wzhzhu/DiffForward/log',new_dir) 


def del_file(path):
    ls = os.listdir(path)
    for i in ls:
        c_path = os.path.join(path, i)
        if os.path.isdir(c_path):
            del_file(c_path)
        else:
            os.remove(c_path)


def start_process2(ip, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
    ssh.connect(ip,22,'k8s')
    # logging.info('ip '+ip+ ' '+cmd)
    stdin,stdout,stderr = ssh.exec_command(cmd)
    outmsg,errmsg = stdout.read(),stderr.read()
    logging.info(errmsg)
    # logging.info(outmsg)
    ssh.close()

def main(argv):
    setting_path_list = list()
    setting_path_list.append('/home/k8s/wzhzhu/DiffForward/Properties/config.ini')
    exp_num = len(setting_path_list)
    for i in range(0, exp_num):
        try:
            shutil.copyfile(setting_path_list[i],'/home/k8s/wzhzhu/DiffForward/src/config.ini')
            reader = ConfigReader()
            exp_name = reader.get('Other Settings','exp_name')
            logging.info('############################')
            logging.info('exp{0}：{1}'.format(i+1, exp_name))
            logging.info('############################')
            '''restrain the bandwidth'''
            os.system('nohup sudo python3 /home/k8s/wzhzhu/DiffForward/src/Manager.py > manager_output 2> manager_output &')
            time.sleep(2)
            mc = ManagerClient()
            bandwidth_max = int(reader.get('Other Settings','bandwidth_max'))
            is_restrict_bandwidth = int(reader.get('Other Settings','is_restrict_bandwidth'))
            if is_restrict_bandwidth == 1:
                mc.restrict_bandwidth(int(bandwidth_max))
            '''start'''
            mc.init_merge_speed_controller()
            mc.init_storage_server()
            is_replication = int(reader.get('Other Settings','replication'))
            if is_replication == 0:
                mc.init_buffer_process()
            mc.init_forwarding_process()
            mc.init_log_merger()
            mc.init_replayer()
            mc.start_replayer()
            logging.info('############################')
            logging.info('Start Replaying')
            logging.info('############################')
            exp_time = int(reader.get('Other Settings','exp_time'))
            time.sleep(exp_time)
            logging.info('############################')
            logging.info('Statistical Results:')
            logging.info('############################')
            schedule_count = mc.get_schedule_count()
            logging.info('migrating {0} times'.format(schedule_count))
            logging.info('clear all processes')
            mc.clean_sys()
            '''cancel bandwidth limitation'''
            mc.unrestrict_bandwidth()
            '''shutdown manager'''
            mc.shutdown_manager()
            '''move experimental results to exp_data dir'''
            copy_exp_result(exp_name)
            '''clear logs'''
            del_file('/home/k8s/wzhzhu/DiffForward/log')
        except Exception as err:
            logging.info(err)
            send_mail('Something went wrong!','')
            sys.exit(0)

    logging.info('Experiments complete！')

if __name__ == "__main__":
    main(sys.argv[1:])


