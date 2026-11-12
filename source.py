import base64
import hashlib
import json
import socket

def packets(fixture):
    contents=[part.encode() for part in fixture['parts']]
    complete=hashlib.sha256(b''.join(contents)).hexdigest()
    output=[]
    for index,content in enumerate(contents):
        frame={'device':fixture['device'],'boot':fixture['boot'],
               'sequence':fixture['sequence'],'index':index,'count':len(contents),
               'part_sha256':hashlib.sha256(content).hexdigest(),
               'group_sha256':complete,'payload':base64.b64encode(content).decode()}
        output.append(json.dumps(frame).encode())
    return output

def send_udp(destination,frames):
    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as channel:
        channel.bind(('127.0.0.1',0))
        address=channel.getsockname()
        for frame in frames:
            channel.sendto(frame,destination)
        return address

class Retention:
    def __init__(self,fixture):
        self.fixture=fixture
        self.frames=packets(fixture)
        self.expired=False

    def missing(self,device,boot,sequence,indexes):
        identity=(self.fixture['device'],self.fixture['boot'],self.fixture['sequence'])
        if self.expired or (device,boot,sequence)!=identity:
            return []
        return [self.frames[index] for index in indexes if 0<=index<len(self.frames)]
