import subprocess
from http.server import HTTPServer, SimpleHTTPRequestHandler
import fcntl, os
import time
from mp4frag import mp4frag

SIZE1 = "1024x576"

_cmd1 = "ffmpeg -threads 2 -loglevel warning -analyzeduration 1000000 -probesize 1000000 -fflags +igndts -i rtmp://media3.sinovision.net:1935/live/livestream -f rawvideo -pix_fmt yuv420p -s %s pipe:1"%SIZE1

_cmd2 = "ffmpeg -threads 2 -loglevel warning -analyzeduration 1000000 -probesize 1000000 -f rawvideo -pix_fmt yuv420p -s %s -i - -i ffmpeg-logo.png -filter_complex overlay=x='if(gte(t,0),-w+(mod(n,W+w))+1,NAN)':y=0 -f rawvideo -pix_fmt yuv420p -s %s pipe:1"%(SIZE1,SIZE1)

_cmd3 = "ffmpeg -threads 2 -loglevel warning -analyzeduration 1000000 -probesize 1000000 -f rawvideo -pix_fmt yuv420p -s %s -i - -f mp4 -vcodec libx264 -strict -2 -movflags +frag_keyframe+empty_moov+default_base_moof -metadata title='FFStream' -reset_timestamps 1 -crf 15 -preset ultrafast -tune zerolatency pipe:1 s.mp4 -y"%SIZE1

def putStream(f):
	print("stream start...")
	init = _mp4frag.initialization
	if init:
		print("set init info")
		print(init.hex())
		f.write(init)
	while True:
		if(f.closed):
			print("file close")
			break
		data = _mp4frag.pipe().recv_bytes()
		if data:
			print(len(data))
			try:
				f.write(data)
			except IOError:
				print("IO error")
				break
		time.sleep(0.1)
	print("stream end")


class Resquest(SimpleHTTPRequestHandler):
	def do_GET(self):
		if self.path.endswith("s.mp4"):
			self.send_response(200)
			self.send_header('Access-Control-Allow-Origin', '*')
			self.send_header('Connection', 'close')
			self.send_header('Cache-Control', 'private, no-cache, no-store, must-revalidate')
			self.send_header('Expires', '-1')
			self.send_header('Pragma', 'no-cache')
			self.send_header('Content-Type', 'video/mp4')
			self.end_headers()
			putStream(self.wfile)
		else:
			SimpleHTTPRequestHandler.do_GET(self)

if __name__ == '__main__':
	cmd1 = _cmd1.split(' ')
	print(cmd1)
	cmd2 = _cmd2.split(' ')
	print(cmd2)
	cmd3 = _cmd3.split(' ')
	print(cmd3)

	ffmpeg1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, bufsize=10000)
	flags = fcntl.fcntl(ffmpeg1.stdout, fcntl.F_GETFL)
	fcntl.fcntl(ffmpeg1.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

	ffmpeg2 = subprocess.Popen(cmd2, stdin=ffmpeg1.stdout, stdout=subprocess.PIPE, bufsize=10000)
	flags = fcntl.fcntl(ffmpeg2.stdout, fcntl.F_GETFL)
	fcntl.fcntl(ffmpeg2.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

	ffmpeg3 = subprocess.Popen(cmd3, stdin=ffmpeg2.stdout, stdout=subprocess.PIPE)
	flags = fcntl.fcntl(ffmpeg3.stdout, fcntl.F_GETFL)
	fcntl.fcntl(ffmpeg3.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

	options = {
		'hlsBase':'test',
		'hlsListSize':5,
		'hlsListInit':True,
		'bufferListSize':5
	}
	_mp4frag = mp4frag(ffmpeg3.stdout, options)
	_mp4frag.start()

	host = ('0.0.0.0', 8888)
	server = HTTPServer(host, Resquest)
	print("Starting server, listen at: %s:%s" % host)
	server.serve_forever()

	_mp4frag.stop()
