# coding=utf-8
import sys
import random
import time
import pandas as pd
import logging
import collections
from gevent.event import Event
import gevent

INTREE = 1
OUTTREE = 2

class TreeNode(object):
    def __init__(self, log_id, offset, length, file_id, file_offset, buffer_server_id):
        self.log_id = log_id
        self.offset = offset
        self.length = length
        self.file_id = file_id
        self.file_offset = file_offset
        self.buffer_server_id = buffer_server_id
        self.left = None
        self.right = None
        self.parent = None
        self.back = None
        self.forward = None
        self.state = INTREE
        self.is_obsolete = False
        self.color = 'black'

        self.immutable_offset = offset
        self.immutable_length = length

class LightweightIndex(object):
    def __init__(self):
        self.null = TreeNode(0, 0, 0, 0, 0, 0)
        self.root = self.null
        self.log_tail = None
        self.log_head = None
        self.merge_log_to_tree_event = Event()
        self.insert_queue = collections.deque()
        self.log_queue = collections.deque()
        self.last_gc_time = 0
        self.filter = dict()

    def LeftRotate(self, x):
        y = x.right
        x.right = y.left
        if y.left != self.null:
            y.left.parent = x
        y.parent = x.parent
        if x.parent == self.null: 
            self.root = y
        elif x is x.parent.left:  
            x.parent.left = y
        else:
            x.parent.right = y
        y.left = x
        x.parent = y

    def RightRotate(self, y):
        x = y.left
        y.left = x.right
        if x.right != self.null: 
            x.right.parent = y
        x.parent = y.parent
        if y.parent == self.null:
            self.root = x
        elif y is y.parent.right:
            y.parent.right = x
        else:
            y.parent.left = x
        x.right = y
        y.parent = x

    def InsertNodeFixup(self, z):
        while z.parent.color == 'red': 
            if z.parent == z.parent.parent.left:    
                y = z.parent.parent.right
                if y.color == 'red':
                    z.parent.color = 'black'
                    y.color = 'black'
                    z.parent.parent.color = 'red'
                    z = z.parent.parent
                else:
                    if z == z.parent.right:
                        z = z.parent
                        self.LeftRotate(z)
                    z.parent.color = 'black'
                    z.parent.parent.color = 'red'
                    self.RightRotate(z.parent.parent)
            else:                                  
                y = z.parent.parent.left   
                if y.color == 'red':
                    z.parent.color = 'black'
                    y.color = 'black'
                    z.parent.parent.color = 'red'
                    z = z.parent.parent
                else:
                    if z == z.parent.left:
                        z = z.parent
                        self.RightRotate(z)
                    z.parent.color = 'black'
                    z.parent.parent.color = 'red'
                    self.LeftRotate(z.parent.parent)
        self.root.color = 'black'

    def RBTransplant(self, u, v):
        if u.parent == self.null:
            self.root = v
        elif u is u.parent.left:
            u.parent.left = v
        else:
            u.parent.right = v
        v.parent = u.parent

    def TreeMinimum(self, x):
        while x.left != self.null:
            # print(x.log_id)
            x = x.left
        return x

    def DeleteNodeFixup(self, x):
        while x is not self.root and x.color == 'black':
            if x is x.parent.left:
                w = x.parent.right 
                if w.color == 'red':     
                    w.color = 'black'
                    x.parent.color = 'red'
                    self.LeftRotate(x.parent)
                    w = x.parent.right
                if w.left.color == 'black' and w.right.color == 'black':
                    w.color = 'red'
                    x = x.parent
                else:
                    if w.right.color == 'black':
                        w.left.color = 'black'
                        w.color = 'red'
                        self.RightRotate(w)
                        w = x.parent.right
                    w.color = x.parent.color
                    x.parent.color = 'black'
                    w.right.color = 'black'
                    self.LeftRotate(x.parent)
                    x = self.root
            else:
                w = x.parent.left
                if w.color == 'red':
                    w.color = 'black'
                    x.parent.color = 'red'
                    self.RightRotate(x.parent)
                    w = x.parent.left
                if w.right.color == 'black' and w.left.color == 'black':
                    w.color = 'red'
                    x = x.parent
                else:
                    if w.left.color == 'black':
                        w.right.color = 'black'
                        w.color = 'red'
                        self.LeftRotate(w)
                        w = x.parent.left
                    w.color = x.parent.color
                    x.parent.color = 'black'
                    w.left.color = 'black'
                    self.RightRotate(x.parent)
                    x = self.root
        x.color = 'black'

    def disable(self, composite_key1, composite_key2):
        offset1, offset2 = composite_key1[0],composite_key2[0]
        length1, length2 = composite_key1[1],composite_key2[1]
        if offset2 <= offset1:
            if offset2+length2>=offset1+length1:
                return None
            else:
                return [(offset2+length2, offset1+length1-(offset2+length2))]
        else:
            if offset2+length2>=offset1+length1:
                return [(offset1, offset2 - offset1)]
            else:
                return [(offset1, offset2 - offset1),(offset2 + length2, offset1 + length1 - (offset2 + length2))]

    def next(self, x):
        if x.right != self.null:
            p = x.right
            while p.left != self.null:
                p = p.left
            return p
        else:
            p = x.parent
            while p != self.null and p.left is not x:
                x = p
                p = p.parent
            return p
    
    '''Judge whether two composite_key have intersection'''
    def isCross(self, composite_key1, composite_key2):
        offset1, offset2 = composite_key1[0],composite_key2[0]
        length1, length2 = composite_key1[1],composite_key2[1]
        if offset1 >= offset2+length2 or offset2 >= offset1+length1:
            return False
        else:
            return True

    '''Get intersection of two composite keys'''
    def intersection(self, composite_key1, composite_key2):
        offset1, offset2 = composite_key1[0],composite_key2[0]
        length1, length2 = composite_key1[1],composite_key2[1]
        offset = max(offset1, offset2)
        length = min(offset1+length1, offset2+length2) - offset
        return (offset, length)

    def printTree(self, root, depth):
        if root == self.null:
            return 
        pre = depth*' '
        self.printTree(root.left, depth + 1)
        print(pre+str(root.log_id), end = ' ')
        print(pre+str(root.offset), end = ' ')
        print(pre+str(root.length), end = ' ')
        print(pre+str(root.file_offset), end = ' ')
        print()
        self.printTree(root.right, depth + 1)

    def backChain(self, log_tail):
        if not log_tail:
            return []
        print(log_tail.log_id, end = ' ')
        print(log_tail.offset, end = ' ')
        print(log_tail.length, end = ' ')
        print()
        return [(log_tail.log_id, log_tail.offset, log_tail.length)]+self.backChain(log_tail.back)

    def logNodeNum(self, flag):
        node_num = 0
        p = self.log_tail
        while p is not None:
            if flag == 0:
                node_num += 1
            else:
                if p.state == INTREE:
                    node_num += 1
            p = p.back
        return node_num
    
    def treeNodeNum(self, root):
        if root == self.null:
            return 0
        return self.treeNodeNum(root.left) + self.treeNodeNum(root.right) + 1

    def insert(self, log_id, offset, length, file_id, file_offset, buffer_server_id):
        self.async_insert(log_id, offset, length, file_id, file_offset, buffer_server_id)
        start = offset//(64*1024)
        end = (offset+length-1)//(64*1024)
        for i in range(start, end + 1):
            if i not in self.filter:
                self.filter[i] = 1
            else:
                self.filter[i] += 1

    def async_insert(self, log_id, offset, length, file_id, file_offset, buffer_server_id):
        self.insert_queue.append((log_id, offset, length, file_id, file_offset, buffer_server_id))
        # self.log_queue.append((log_id, offset, length, file_id, file_offset, buffer_server_id))
        self.merge_log_to_tree_event.set()

    def sync_insert(self, log_id, offset, length, file_id, file_offset, buffer_server_id):
        # self.log_queue.append((log_id, offset, length, file_id, file_offset, buffer_server_id))
        node = TreeNode(log_id, offset, length, file_id, file_offset, buffer_server_id)
        self.insert_to_log(node)
        self.insert_to_tree(node)

    def insert_to_log(self, node):
        node.back = self.log_tail
        node.forward = None
        if self.log_tail is not None:
            self.log_tail.forward = node
        self.log_tail = node
        if self.log_head is None:
            self.log_head = node

    def insert_to_tree(self, node):
        #先把所有重合部分disable掉
        first = self.query_first(self.root, node.offset, node.length)
        while first != self.null:
            composite_key1 = (first.offset, first.length)
            composite_key2 = (node.offset, node.length)
            next_node = self.next(first)
            if not self.isCross(composite_key1,composite_key2):
                break
            key_list = self.disable(composite_key1, composite_key2)
            # modify to disable invalid region
            if key_list is None:
                self.DeleteNode(first)
            else:
                first.file_offset += key_list[0][0] - first.offset
                first.offset, first.length = key_list[0]
                # might be split, if first is not obsolete, do split
                if len(key_list) == 2 and not first.is_obsolete:
                    new_node = TreeNode(first.log_id, key_list[1][0], key_list[1][1], first.file_id, first.file_offset + key_list[1][0] - first.offset, first.buffer_server_id)
                    new_node.back = first
                    new_node.forward = first.forward
                    if first.forward is not None:
                        first.forward.back = new_node
                    first.forward = new_node
                    self.InsertNode(new_node)
                    if self.log_tail is first:
                        self.log_tail = new_node
            first = next_node
        self.InsertNode(node)

    '''Remove obsolote node in index according to given log_head'''
    def rmLog(self, log_id, busy_flag):
        if self.log_tail is None:
            return
        last_log_id = -1
        if self.log_tail.log_id < log_id:
            p = self.log_head
            self.root = self.null
            self.log_tail = None
            self.log_head = None
            while p is not None and p.log_id < log_id:
                gevent.idle()
                # if busy_flag.isbusy():
                #     busy_flag.wait()
                #     continue
                this_node = p
                p = p.forward
                if this_node.log_id != last_log_id:
                    start = this_node.immutable_offset//(64*1024)
                    end = (this_node.immutable_offset+this_node.immutable_length-1)//(64*1024)
                    for i in range(start, end + 1):
                        self.filter[i] -= 1
                        if self.filter[i] == 0:
                            del self.filter[i]
                    last_log_id = this_node.log_id
                # time.sleep(0)
            return
        p = self.log_head
        last_log_id = -1
        while p.log_id < log_id:
            # busy_flag.wait()
            gevent.idle()
            # if busy_flag.isbusy():
            #     busy_flag.wait()
            #     continue
            this_node = p
            p = p.forward
            if this_node.state == INTREE:
                self.DeleteNode(this_node)
            if this_node.forward is not None:
                this_node.forward.back = None
            this_node.forward = this_node.back = None
            if this_node.log_id != last_log_id:
                start = this_node.immutable_offset//(64*1024)
                end = (this_node.immutable_offset+this_node.immutable_length-1)//(64*1024)
                for i in range(start, end + 1):
                    self.filter[i] -= 1
                    if self.filter[i] == 0:
                        del self.filter[i]
                last_log_id = this_node.log_id
            # time.sleep(0)
        self.log_head = p

    def async_merge_log_to_tree(self, busy_flag):
        while True:
            self.merge_log_to_tree_event.wait()
            while len(self.insert_queue) != 0:
                # busy_flag.wait()
                gevent.idle()
                # if busy_flag.isbusy():
                #     busy_flag.wait()
                #     continue
                if len(self.insert_queue) == 0:
                    break
                log_id, offset, length, file_id, file_offset, buffer_server_id = self.insert_queue.popleft()
                node = TreeNode(log_id, offset, length, file_id, file_offset, buffer_server_id)
                self.insert_to_log(node)
                self.insert_to_tree(node)
                # time.sleep(0)
            if len(self.insert_queue) == 0:
                self.merge_log_to_tree_event.clear()

    def sync_merge_log_to_tree(self):
        while len(self.insert_queue) != 0:
            log_id, offset, length, file_id, file_offset, buffer_server_id = self.insert_queue.popleft()
            node = TreeNode(log_id, offset, length, file_id, file_offset, buffer_server_id)
            self.insert_to_log(node)
            self.insert_to_tree(node)

    def range_overlap(self, offset, length):
        if len(self.filter) == 0:
            return False
        start = offset//(64*1024)
        end = (offset+length-1)//(64*1024)
        for i in range(start, end + 1):
            if i in self.filter:
                return True
        return False

    '''Get intersection of given offset and length in the index'''
    def search(self, offset, length):
        result_list = list()
        if len(self.filter) == 0:
            return result_list
        # filter
        need_search = False
        start = offset//(64*1024)
        end = (offset+length-1)//(64*1024)
        for i in range(start, end + 1):
            if i in self.filter:
                need_search = True
                break
        if not need_search:
            return result_list
        self.sync_merge_log_to_tree()
        first = self.query_first(self.root, offset, length)
        while first != self.null:
            composite_key1 = (offset, length)
            composite_key2 = (first.offset, first.length)
            if not self.isCross(composite_key1,composite_key2):
                break
            if not first.is_obsolete:
                intersection = self.intersection((offset,length),(first.offset, first.length))
                result_list.append((intersection[0], intersection[1], first.log_id, first.file_id, first.file_offset + intersection[0]- first.offset, first, first.buffer_server_id))
            first = self.next(first)
        return result_list

    def InsertNode(self, z):
        y = self.null
        x = self.root
        while x != self.null:
            y = x
            if z.offset < x.offset:
                x = x.left
            else:
                x = x.right
        z.parent = y
        if y == self.null:  
            self.root = z
        elif z.offset < y.offset:
            y.left = z
        else:
            y.right = z
        z.left = self.null
        z.right = self.null
        z.color = 'red'
        self.InsertNodeFixup(z)

    def SearchNode(self, root, composite_key):
        if root == self.null:
            return self.null
        offset = composite_key[0]
        length = composite_key[1]
        if root.offset == offset:
            return root
        elif root.offset > offset:
            return self.SearchNode(root.left, composite_key)
        else:
            return self.SearchNode(root.right, composite_key)

    def DeleteNode(self, z):
        if z == self.null:
            return
        y = z        
        y_original_color = y.color
        if z.left == self.null:
            x = z.right       
            self.RBTransplant(z, z.right)
        elif z.right == self.null:
            x = z.left
            self.RBTransplant(z, z.left)
        else:
            y = self.TreeMinimum(z.right)
            y_original_color = y.color
            x = y.right    
            if y.parent is z:
                x.parent = y
            else:               
                self.RBTransplant(y, y.right)
                y.right = z.right
                y.right.parent = y
            self.RBTransplant(z, y)
            y.left = z.left
            y.left.parent = y
            y.color = z.color
        z.state = OUTTREE
        if y_original_color == 'black':
            self.DeleteNodeFixup(x)

    '''Get first node which has intersection with given offset and length in index'''
    def query_first(self, root, offset, length):
        if root == self.null:
            return self.null
        if root.offset + root.length <= offset:
            return self.query_first(root.right, offset, length)  
        elif root.offset >= offset + length:
            return self.query_first(root.left, offset, length)
        elif root.offset <= offset and not root.is_obsolete:
            return root
        else:
            temp = self.query_first(root.left, offset, length)
            if temp != self.null:
                return temp
            else:
                return root