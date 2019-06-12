import cv2
import os
import queue
import grpc
import time
import collections
import ctypes

import uuid
import threading
import logging
from logging.handlers import RotatingFileHandler

import dataformat_pb2
import dataformat_pb2_grpc

from sysInit import *
import numpy as np

# Data buffer uses dataformat_pb2.DataFrame() format
class DataBufQueue():
    def __init__(self, size, sender):
        self.bufQ = queue.Queue(size)
        self.sender = sender

    def PutFull(self, buf):
        self.bufQ.put(buf)

    def IsFull(self):
        return self.bufQ.full() 

    def IsEmpty(self):
        return self.bufQ.empty() 

    def GetFull(self):
        return self.bufQ.get()

    def PutEmptyArray(self, array):
        self.sender.ReleaseUsedArray(array)

    def GetQueueSize(self):
        return self.bufQ.qsize()

class DataStreamCallback(): 
    def __init__(self, logger, context): 
        self.context = context
        self.peer = context.peer()
        self.logger = logger

    def OnRpcStop(self):
        self.logger.info('Peer {} terminated datastrm'.format(self.peer))


# Note that each Sender can multicast data to more than one receivers. 
# Each receiver can only receive data from one sender source
class Sender():
    def __init__(self, logger, blkGuid, portName, dataType):
        self.logger = logger
        self.logger.info("Creating sender {}({})".format(blkGuid, portName))
        self.blkGuid = blkGuid
        self.portName = portName
        self.dataType = dataType
        self.rcvBufQMapLock = threading.Lock()
        self.rcvBufQMap = {}
        self.rcvGuidMap = {}
        self.rcvPortMap = {}
        self.inuseArrayRefMap = {}
        self.inuseArrayCntMap = {}

    def __del__(self):
        self.logger.info("Delete sender {}".format(self.portName))

    def IsConnected(self):
        if len(self.rcvBufQMap) > 0:
            return True
        else:
            return False

    def GetName(self):
        return self.portName

    def GetDataType(self):
        return self.dataType

    def LinkDstBufQ(self, rcvBlkGuid, rcvPort, bufQ):
        with self.rcvBufQMapLock:
            uuid = rcvBlkGuid + rcvPort
            self.rcvBufQMap[uuid] = bufQ
            self.rcvGuidMap[uuid] = rcvBlkGuid 
            self.rcvPortMap[uuid] = rcvPort
            self.logger.info("Sender({}:{} - LinkDstBufQ rcvBlkGuid {} rcvPort {}".format(self.blkGuid, self.portName, rcvBlkGuid, rcvPort))

    def UnlinkDstBufQ(self, rcvBlkGuid, rcvPort):
        with self.rcvBufQMapLock:
            uuid = rcvBlkGuid + rcvPort
            element = self.rcvBufQMap.pop(uuid, None)
            if element is not None:
                del element

            element = self.rcvGuidMap.pop(uuid, None)
            if element is not None:
                del element

            element = self.rcvPortMap.pop(uuid, None)
            if element is not None:
                del element
            self.logger.info("Sender({}:{} - UnlinkDstBufQ".format(self.blkGuid, self.portName))

    def PutFullArray(self, ts, dataFormat, array):
        complete = False
        with self.rcvBufQMapLock:
            for uuid in self.rcvBufQMap:
                dataptr = array.ctypes.data
                dataFrame = dataformat_pb2.DataFrame(senderBlkGuid = self.blkGuid,
                                            senderPort = self.portName,
                                            receiverBlkGuid = self.rcvGuidMap[uuid],
                                            receiverPort = self.rcvPortMap[uuid],
                                            format = dataFormat, 
                                            timestamp = ts, 
                                            dataptr = dataptr)
                bufQ = self.rcvBufQMap[uuid]
                if not bufQ.IsFull():
#                    self.logger.debug("Sender({})->dest({}) bufQ.PutFull ts {} dataptr {:08x}".format(self.portName, uuid, ts, dataptr)) 
                    bufQ.PutFull(dataFrame)
                    if dataptr in self.inuseArrayCntMap:
                        self.inuseArrayCntMap[dataptr] = self.inuseArrayCntMap[dataptr] + 1
                    else:
                        self.inuseArrayRefMap[dataptr] = array  # This is to hold the np array so that memory is not released until all receivers are done using it
                        self.inuseArrayCntMap[dataptr] = 1
                    complete = True
#                else:
#                    self.logger.debug("Sender({})->dest({}) Buffer FULL ts {} dataptr {:08x}".format(self.portName, uuid, ts, dataptr)) 

            if complete == False:       
                time.sleep(0.01)   # Delay a little bit when one of the target buffer is overflowing

    # Keep on blocking if any one of the egress buffer is full
    def PutFullArrayBlocking(self, ts, dataFormat, array):
        putFullDoneList = []
        while True: 
            incomplete = False
            with self.rcvBufQMapLock:
                for uuid in self.rcvBufQMap:
                    if uuid in putFullDoneList:
                        continue 

                    dataptr = array.ctypes.data
                    dataFrame = dataformat_pb2.DataFrame(senderBlkGuid = self.blkGuid,
                                            senderPort = self.portName,
                                            receiverBlkGuid = self.rcvGuidMap[uuid],
                                            receiverPort = self.rcvPortMap[uuid],
                                            format = dataFormat, 
                                            timestamp = ts, 
                                            dataptr = dataptr)
                    bufQ = self.rcvBufQMap[uuid]
                    if bufQ.IsFull():
#                        self.logger.debug("Sender({}) bufQ ({}) FULL ts{} dataptr {:08x}".format(self.portName, uuid, ts, dataptr)) 
                        incomplete = True
                        break

#                    self.logger.debug("Sender({}) bufQ.PutFull ts{} dataptr {:08x}".format(self.portName, ts, dataptr)) 
                    bufQ.PutFull(dataFrame)
                    if dataptr in self.inuseArrayCntMap:
                        self.inuseArrayCntMap[dataptr] = self.inuseArrayCntMap[dataptr] + 1
                    else:
                        self.inuseArrayRefMap[dataptr] = array  # This is to hold the np array so that memory is not released until all receivers are done using it
                        self.inuseArrayCntMap[dataptr] = 1

                    putFullDoneList.append(uuid)

            if incomplete == False:      
                break
            else:
                time.sleep(0.01)

    def ReleaseUsedArray(self, array):
        with self.rcvBufQMapLock:
            dataptr = array.ctypes.data
            if dataptr in self.inuseArrayCntMap:
                self.inuseArrayCntMap[dataptr] = self.inuseArrayCntMap[dataptr] - 1
#                self.logger.info("Sender({}) ReleaseUsedArray dataptr({:08x}), refcnt({}) ".format(self.portName, dataptr, self.inuseArrayCntMap[dataptr]))
                if self.inuseArrayCntMap[dataptr] == 0:
                    del self.inuseArrayCntMap[dataptr]
                    del self.inuseArrayRefMap[dataptr]
           

class Receiver():
    def __init__(self, logger, blkGuid, portName, dataType):
        self.logger = logger
        self.logger.info("Creating receiver {}({})".format(blkGuid, portName))
        self.blkGuid = blkGuid
        self.portName = portName
        self.dataType = dataType
        self.rcvBufQ = None

    def __del__(self):
        self.logger.info("Delete receiver {}({})".format(self.blkGuid, self.portName))

    def GetBlkGuid(self):
        return self.blkGuid

    def GetName(self):
        return self.portName

    def GetDataType(self):
        return self.dataType

    def IsConnected(self):
        if self.rcvBufQ != None:
            return True
        else:
            return False

    def LinkSrcBufQ(self, bufQ):
        self.rcvBufQ = bufQ

    def UnlinkSrcBufQ(self):
        self.rcvBufQ = None

    def IsEmpty(self):
        return self.rcvBufQ.IsEmpty()

    def IsFull(self):
        return self.rcvBufQ.IsFull()

    def GetQueueSize(self):
        return self.rcvBufQ.GetQueueSize()

    def GetFullArray(self):
        buf = self.rcvBufQ.GetFull()
#        self.logger.debug("Receiver({}) GetFull dataptr {}".format(self.portName, buf.dataptr))
        if buf.format.pixelFormat == "CV_64FC3":
            dataptr = ctypes.cast(buf.dataptr, ctypes.POINTER(ctypes.c_double))
        elif buf.format.pixelFormat == "CV_32FC3":
            dataptr = ctypes.cast(buf.dataptr, ctypes.POINTER(ctypes.c_float))
        else:
            dataptr = ctypes.cast(buf.dataptr, ctypes.POINTER(ctypes.c_uint8))
        shape = ()
        for dim in buf.format.dimSize:
            shape = shape + (dim, )
        array = np.ctypeslib.as_array(dataptr, shape=shape)
#        self.logger.debug("Receiver({}) GetFullArray ts({}) dataptr({:08x})".format(self.portName, buf.timestamp, array.ctypes.data))
        return buf.timestamp, buf.format, array

    def PutUsedArray(self, array):
#        self.logger.debug("Receiver({}) PutUsedArray {:08x}".format(self.portName, array.ctypes.data))
        self.rcvBufQ.PutEmptyArray(array)

def ConnectPorts(logger, srcPort, dstPort, bufQSize):
    srcDataType = srcPort.GetDataType()
    dstDataType = dstPort.GetDataType()
    if srcDataType != dstDataType:
        logger.error("Src DataType {} mismatch Dst DataType {}".format(srcDataType, dstDataType))
        return False
    bufQ = DataBufQueue(bufQSize, srcPort)
    srcPort.LinkDstBufQ(dstPort.GetBlkGuid(), dstPort.GetName(), bufQ)
    dstPort.LinkSrcBufQ(bufQ)
    return True


def DisconnectPorts(logger, srcPort, dstPort):
    while dstPort.IsEmpty() == False:
        ts, fmt, tmp = dstPort.GetFullArray() 
        dstPort.PutUsedArray(tmp)
    srcPort.UnlinkDstBufQ(dstPort.GetBlkGuid(), dstPort.GetName())
    dstPort.UnlinkSrcBufQ()
