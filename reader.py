import base64
import hashlib
import json

def decode_packet(raw):
    frame=json.loads(raw)
    payload=base64.b64decode(frame['payload'],validate=True)
    if hashlib.sha256(payload).hexdigest()!=frame['part_sha256']:
        raise ValueError('分片摘要不符')
    return frame,payload

def inspect(frames,fixture):
    frame,payload=decode_packet(frames[0])
    return {'设备':frame['device'],'片号':frame['index'],'字节数':len(payload)}
