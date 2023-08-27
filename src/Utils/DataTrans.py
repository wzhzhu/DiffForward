#!/usr/bin/python2
#coding:utf-8
import struct
import time
import logging
import socket

logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )

def send(content, sock):
    content_length = len(content)
    content = struct.pack('>II', 666, content_length + 4 +4) + content
    sock.sendall(content)

def batch_send(send_list, sock):
    if len(send_list) == 1:
        send(send_list[0], sock)
        return
    batch_content = bytearray()
    for content in send_list:
        content_length = len(content)
        batch_content.extend(struct.pack('>II', 666, content_length + 4 +4))
        batch_content.extend(content)
    sock.sendall(batch_content)

def recv(sock, target = None):
    # print('recv all')
    content = sock.recv(8)
    if target is not None:
        target.add()
    recv_length = len(content)
    if recv_length == 0:
        pass
    else:
        while recv_length < 8:
            content += sock.recv(8-recv_length)
            recv_length = len(content)
    try:
        verify_code, content_length = struct.unpack('>II', content[0:8])
        if verify_code != 666:
            raise Exception('verify code wrong!')
        recv_length = len(content)
        content_list = list()
        while recv_length < content_length:
            part_content = sock.recv(content_length-recv_length)
            if part_content is not None:
                content_list.append(part_content)
                recv_length += len(part_content)
            # print('recv length',str(recv_length))
        return bytearray().join(content_list)
    except Exception as err:
        raise err
    finally:
        if target is not None:
            target.sub()
