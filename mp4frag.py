import time
from io import BytesIO
from multiprocessing import Process,Pipe
import threading

class mp4frag(threading.Thread):
	'''
	Creates a stream transform for piping a fmp4 (fragmented mp4) from ffmpeg.
	Can be used to generate a fmp4 m3u8 HLS playlist and compatible file fragments.
	Can also be used for storing past segments of the mp4 video in a buffer for later access.
	Must use the following ffmpeg flags <b><i>-movflags +frag_keyframe+empty_moov</i></b> to generate a fmp4
	with a compatible file structure : ftyp+moov -> moof+mdat -> moof+mdat -> moof+mdat ...
	'''

	_FTYP = bytes([0x66, 0x74, 0x79, 0x70])# ftyp
	_MOOV = bytes([0x6d, 0x6f, 0x6f, 0x76])# moov
	_MOOF = bytes([0x6d, 0x6f, 0x6f, 0x66])# moof
	_MFRA = bytes([0x6d, 0x66, 0x72, 0x61])# mfra
	_MDAT = bytes([0x6d, 0x64, 0x61, 0x74])# mdat
	_MP4A = bytes([0x6d, 0x70, 0x34, 0x61])# mp4a
	_AVCC = bytes([0x61, 0x76, 0x63, 0x43])# avcC

	def __init__(self, pipe, options = None):
		'''constructor
		:param pipe: a pipe to input stream data
		:param options: Configuration options.
		:param options['hlsBase']: Base name of files in fmp4 m3u8 playlist. Affects the generated m3u8 playlist by naming file fragments. Must be set to generate m3u8 playlist.
		:param options['hlsListSize']: Number of segments to keep in fmp4 m3u8 playlist. Must be an integer ranging from 2 to 10. Defaults to 4 if hlsBase is set and hlsListSize is not set.
		:param options['hlsListInit']: Indicates that m3u8 playlist should be generated after init segment is created and before media segments are created. Defaults to false.
		:param options['bufferListSize']: Number of segments to keep buffered. Must be an integer ranging from 2 to 10. Not related to HLS settings.
		'''
		threading.Thread.__init__(self)
		if (options and isinstance(options, dict)):
			if (('hlsBase' in options) and isinstance(options['hlsBase'],str)):
				if (('hlsListSize' in options) and isinstance(options['hlsListSize'],int)):
					if (options['hlsListSize'] < 2):
						self._hlsListSize = 2
					elif (options['hlsListSize'] > 10):
						self._hlsListSize = 10
					else:
						self._hlsListSize = options['hlsListSize']
				else:
					self._hlsListSize = 4
				self._hlsList = []
				self._hlsBase = options['hlsBase']
				self._sequence = -1
				if ('hlsListInit' in options):
					self._hlsListInit = (options['hlsListInit'] == True)
				else:
					self._hlsListInit = False
			if ('bufferListSize' in options):
				if (isinstance(options['hlsListSize'],int)):
					if (options['hlsListSize'] > 10):
						self._bufferListSize = 10
					elif (options['hlsListSize'] < 2):
						self._bufferListSize = 2
					else:
						self._bufferListSize = options['hlsListSize']
				else:
					self._bufferListSize = 2
				self._bufferList = []
		self._parseChunk = self._findFtyp
		self._rpipe = pipe
		self._pipe,self._wpipe = Pipe(duplex=True)
		self._run = True

	def __del__(self):
		self._pipe.close()
		self._wpipe.close()
		self._hlsList = []
		self._bufferList = []

	def stop(self):
		self._run = False
		self.join()

	def _int(self, data):
		return int.from_bytes(data,byteorder='big',signed=False)

	def pipe(self):
		'''
		get output pipe
		'''
		return self._wpipe

	def run(self):
		while self._run:
			self._transform()
			time.sleep(0.1)

	def _transform(self):
		chunk = self._rpipe.read()
		if (chunk):
			self._parseChunk(chunk)

	def _bsconcate(self, li, leng = 0):
		'''
		concate bytes array
		'''
		res = bytes()
		for	i in li:
			res += i
		if (leng <= 0) or (leng > len(res)):
			return res
		return res[:leng]

	@property
	def mime(self):
		'''
		Returns the mime codec information as a String.
		Returns <b>Null</b> if requested before [initialized event]
		'''
		return self._mime if (hasattr(self, '_mime')) else None
	
	@property
	def initialization(self):
		'''
		Returns the mp4 initialization fragment as a Buffer.
		Returns <b>Null</b> if requested before [initialized event]
		'''
		return self._initialization if (hasattr(self, '_initialization')) else None

	@property
	def segment(self):
		'''
		Returns the latest Mp4 segment as a Buffer.
		Returns <b>Null</b> if requested before [segment event]
		'''
		return self._setSegment if (hasattr(self, '_setSegment')) else None

	@property
	def timestamp(self):
		'''
		Returns the timestamp of the latest Mp4 segment as an Integer(milliseconds).
		Returns <b>-1</b> if requested before first [segment event]
		'''
		return self._timestamp if (hasattr(self, '_timestamp')) else -1

	@property
	def duration(self):
		'''
		Returns the duration of latest Mp4 segment as a Float(seconds).
		Returns <b>-1</b> if requested before first [segment event]
		'''
		return self._duration if (hasattr(self, '_duration')) else -1
	
	@property
	def m3u8(self):
		'''
		Returns the fmp4 HLS m3u8 playlist as a String.
		Returns <b>Null</b> if requested before [initialized event]
		'''
		return self._m3u8 if (hasattr(self, '_m3u8')) else None
	
	@property
	def sequence(self):
		'''
		Returns the latest sequence of the fmp4 HLS m3u8 playlist as an Integer.
		Returns <b>-1</b> if requested before first [segment event]
		'''
		return self._sequence if (hasattr(self, '_m3u8') and isinstance(self._sequence, int)) else -1
	
	@property
	def bufferList(self):
		'''
		Returns the buffered mp4 segments as an Array.
		Returns <b>Null</b> if requested before first [segment event]
		'''
		return self._bufferList if (hasattr(self, '_bufferList') and (len(self._bufferList) > 0)) else None

	def bufferListConcat(self):
		'''
		Returns the [mp4frag.bufferList] concatenated as a Buffer
		Returns <b>Null</b> if requested before first [segment event]
		'''
		if(hasattr(self, '_bufferList') and (len(self._bufferList) > 0)):
			return self._bsconcate(self._bufferList)
		return None

	def bufferConcat(self):
		'''
		Returns the [mp4frag.initialization] and [mp4frag.bufferList] concatenated as a Buffer.
		Returns <b>Null</b> if requested before first [segment event]
		'''
		if(hasattr(self, '_initialization') and hasattr(self, '_bufferList') and (len(self._bufferList) > 0)):
			return self._bsconcate([self._initialization] + self._bufferList)
		return None

	def getHlsSegment(self, sequence):
		'''
		Returns the Mp4 segment that corresponds to the HLS numbered sequence as a Buffer.
		Returns <b>Null</b> if there is no .m4s segment that corresponds to sequence number.
		'''
		return self.getHlsNamedSegment('%s%d.m4s'%(self._hlsBase, sequence))
	
	def getHlsNamedSegment(self, name):
		'''Returns the Mp4 segment that corresponds to the HLS named sequence as a Buffer.
		Returns <b>Null</b> if there is no .m4s segment that corresponds to sequence name.
		'''
		if (name and hasattr(self, '_hlsList') and (len(self._hlsList) > 0)):
			for i in range(0, len(self._hlsList)):
				if (self._hlsList[i].name == name):
					return self._hlsList[i].segment
		return None

	def _findFtyp(self, chunk):
		'''
		Search buffer for ftyp.
		'''
		print('_findFtyp')
		chunkLength = len(chunk)
		if (chunkLength < 8 or chunk.find(mp4frag._FTYP) != 4):
			print("error: %s no fount"%mp4frag._FTYP.decode())
			return
		self._ftypLength = self._int(chunk[:4])
		if (self._ftypLength < chunkLength):
			self._ftyp = chunk[:self._ftypLength]
			self._parseChunk = self._findMoov
			self._parseChunk(chunk[self._ftypLength:])
		elif (self._ftypLength == chunkLength):
			self._ftyp = chunk
			self._parseChunk = self._findMoov
		else:
			#hould not be possible to get here because ftyp is approximately 24 bytes
			#will have to buffer this chunk and wait for rest of it on next pass
			print("error: ftypLength:%d > chunkLength:%d"%(self._ftypLength, chunkLength))

	def _findMoov(self, chunk):
		'''
		Search buffer for moov.
		'''
		print('_findMoov')
		chunkLength = len(chunk)
		if (chunkLength < 8 or chunk.find(mp4frag._MOOV) != 4):
			print("error: %s not found."%mp4frag._MOOV.decode())
			return
		moovLength = self._int(chunk[:4])
		if (moovLength < chunkLength):
			self._parseMoov(self._ftyp + chunk)
			del self._ftyp
			del self._ftypLength
			self._parseChunk = self._findMoof
			self._parseChunk(chunk[moovLength:])
		elif (moovLength == chunkLength):
			self._parseMoov(self._ftyp + chunk)
			del self._ftyp
			del self._ftypLength
			self._parseChunk = self._findMoof
		else:
			#probably should not arrive here here because moov is typically < 800 bytes
			#will have to store chunk until size is big enough to have entire moov piece
			#ffmpeg may have crashed before it could output moov and got us here
			print("error: moovLength:%d > chunkLength:%d"%(moovLength, chunkLength))

	def _findMoof(self, chunk):
		'''
		Search buffer for moof.
		'''
		print('_findMoof')
		if (hasattr(self, '_moofBuffer')):
			self._moofBuffer.append(chunk)
			chunkLength = len(chunk)
			self._moofBufferSize += chunkLength
			if (self._moofLength == self._moofBufferSize):
				self._moof = self._bsconcate(self._moofBuffer, self._moofLength)
				del self._moofBuffer
				del self._moofBufferSize
				self._parseChunk = self._findMdat
			elif (self._moofLength < self._moofBufferSize):
				self._moof = self._bsconcate(self._moofBuffer, self._moofLength)
				sliceIndex = chunkLength - (self._moofBufferSize - self._moofLength)
				del self._moofBuffer
				del self._moofBufferSize
				self._parseChunk = self._findMdat
				self._parseChunk(chunk[sliceIndex:])
		else:
			chunkLength = len(chunk)
			if (chunkLength < 8 or chunk.find(mp4frag._MOOF) != 4):
				#ffmpeg occasionally pipes corrupt data, lets try to get back to normal if we can find next MOOF box before attempts run out
				mfraIndex = chunk.find(mp4frag._MFRA)
				if mfraIndex != -1:
					return
				self._moofHunts = 0
				self._moofHuntsLimit = 40
				self._parseChunk = self._moofHunt
				self._parseChunk(chunk)
				return
			self._moofLength = self._int(chunk[:4])
			if self._moofLength == 0:
				print("error, Bad data from input stream reports %s length of 0"%mp4frag._MOOF.decode())
				return
			if self._moofLength < chunkLength:
				self._moof = chunk[:self._moofLength]
				self._parseChunk = self._findMdat
				self._parseChunk(chunk[self._moofLength:])
			elif self._moofLength == chunkLength:
				self._moof = chunk
				self._parseChunk = self._findMdat
			else:
				self._moofBuffer = [chunk]
				self._moofBufferSize = chunkLength

	def _findMdat(self, chunk):
		'''
		Search buffer for mdat.
		'''
		print('_findMdat')
		if (hasattr(self, '_mdatBuffer')):
			self._mdatBuffer.append(chunk)
			chunkLength = len(chunk)
			self._mdatBufferSize += chunkLength
			if (self._mdatLength == self._mdatBufferSize):
				self._setSegment(self._bsconcate([self._moof] + self._mdatBuffer, self._moofLength + self._mdatLength))
				del self._moof
				del self._mdatBuffer
				del self._mdatBufferSize
				del self._mdatLength
				del self._moofLength
				self._parseChunk = self._findMoof
			elif (self._mdatLength < self._mdatBufferSize):
				self._setSegment(self._bsconcate([self._moof] + self._mdatBuffer, self._moofLength + self._mdatLength))
				sliceIndex = chunkLength - (self._mdatBufferSize - self._mdatLength)
				del self._moof
				del self._mdatBuffer
				del self._mdatBufferSize
				del self._mdatLength
				del self._moofLength
				self._parseChunk = self._findMoof
				self._parseChunk(chunk[sliceIndex:])
		else:
			chunkLength = len(chunk)
			if (chunkLength < 8 or chunk.find(mp4frag._MDAT) != 4):
				print("error: %s no found"%mp4frag._MDAT.decode())
				return
			self._mdatLength = self._int(chunk[:4])
			if (self._mdatLength > chunkLength):
				self._mdatBuffer = [chunk]
				self._mdatBufferSize =chunkLength
			elif (self._mdatLength < chunkLength):
				self._setSegment(self._bsconcate([self._moof, chunk], self._moofLength + chunkLength))
				del self._moof
				del self._moofLength
				del self._mdatLength
				self._parseChunk = self._findMoof
			else:
				self._setSegment(self._bsconcate([self._moof, chunk], self._moofLength + self._mdatLength))
				sliceIndex = self._mdatLength
				del self._moof
				del self._moofLength
				del self._mdatLength
				self._parseChunk = self._findMoof
				self._parseChunk(chunk[sliceIndex:])

	def _moofHunt(self, chunk):
		'''
		Find moof after miss due to corrupt data in pipe.
		'''
		print('_moofHunt')
		if (self._moofHunts < self._moofHuntsLimit):
			self._moofHunts += 1
			index = chunk.find(mp4frag._MOOF)
			if (index > 3 and len(chunk) > index + 3):
				del self._moofHunts
				del self._moofHuntsLimit
				self._parseChunk = self._findMoof
				self._parseChunk(chunk[index - 4:])
		else:
			print("error: %s hunt failed afer %d attempts."%(mp4frag._MOOF.decode(), self._moofHunts))

	def _parseMoov(self, chunk):
		'''
		Parse moov for mime.
		'''
		print('_parseMoov')
		self._initialization = chunk
		if (not hasattr(self, '_initialization')):
			print('not has _initialization')
		audioString = ''
		if (self._initialization.find(mp4frag._MP4A) != -1):
			audioString = ', mp4a.40.2'
		index = self._initialization.find(mp4frag._AVCC)
		if index == -1:
			print("error: %s codec info not found"%mp4frag._AVCC.decode())
			return
		index += 5
		self._mime = "video/mp4; codecs='avc1.%s%s'"%( \
			self._initialization[index:index + 3].hex().upper(), \
			audioString \
		)
		self._timestamp = int(round(time.time() * 1000))
		if (hasattr(self, '_hlsList') and hasattr(self, '_hlsListInit')):
			m3u8 = '#EXTM3U\n'
			m3u8 += '#EXT-X-VERSION:7\n'
			#m3u8 += '#EXT-X-ALLOW-CACHE:NO\n'
			m3u8 += '#EXT-X-TARGETDURATION:1\n'
			m3u8 += '#EXT-X-MEDIA-SEQUENCE:0\n'
			m3u8 += '#EXT-X-MAP:URI="init-%s.mp4"\n'%self._hlsBase
			self._m3u8 = m3u8
		print("initialized.")

	def _setSegment(self, chunk):
		'''
		Process current segment.
		'''
		self._segment = chunk
		currentTime = int(round(time.time() * 1000))
		self._duration = max((currentTime - self._timestamp)/1000, 1)
		self._timestamp = currentTime
		if (hasattr(self, '_hlsList')):
			self._sequence += 1
			self._hlsList.append({'sequence': self._sequence,
				'name':'%s%d'%(self._hlsBase, self._sequence),
				'segment':self._sequence,
				'duration':self._duration})
			while (len(self._hlsList) > self._hlsListSize):
				self._hlsList.pop(0)
			m3u8 = '#EXTM3U\n'
			m3u8 += '#EXT-X-VERSION:7\n'
			#m3u8 += '#EXT-X-ALLOW-CACHE:NO\n'
			m3u8 += '#EXT-X-TARGETDURATION:%d\n'%round(self._duration)
			m3u8 += '#EXT-X-MEDIA-SEQUENCE:%d\n'%self._hlsList[0]['sequence']
			m3u8 += '#EXT-X-MAP:URI="init-%s.mp4"\n'%self._hlsBase
			for i in range(0, len(self._hlsList)):
				m3u8 += '#EXTINF:%d'%self._hlsList[i]['duration']
				m3u8 += '%s'%self._hlsList[i]['name']
			self._m3u8 = m3u8
		if (hasattr(self, '_bufferList')):
			self._bufferList.append(self._segment)
			while (len(self._bufferList) > self._bufferListSize):
				self._bufferList.pop(0)
		#Fires when the latest Mp4 segment is parsed from the piped data.
		print("send segment.")
		self._pipe.send_bytes(self._segment)