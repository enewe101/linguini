import os
from datetime import datetime


class ResourceException(Exception):
	pass


class Resource(object):

	def get_ready(self, lot, as_pilot):

		self.lot = lot
		self.as_pilot = as_pilot
		self._is_ready = True


	def is_ready(self):
		try:
			return self._is_ready
		except AttributeError:
			return False


	def exists(self):
		raise NotImplementedError(
			'Resources need to implement an `exists` method.'
		)


class FileResource(Resource):

	def __init__(self, path, fname):
		self.path = path
		self.fname = fname


	def get_path(self):

		# ensure the resource is ready
		if not self.is_ready():
			raise ResourceException('Resource is not ready.')

		fname = (
			self.lot + '_' 
			+ ('pilot' if self.as_pilot else '') 
			+ self.fname
		)
		return os.path.join(self.path, fname)


	def exists(self):
		return os.path.isfile(self.get_path())

	def open(self, flags):

		# there's some things to check if we're writing to a *new* file
		if 'w' in flags:

			# if writing, check whether the file exists
			if os.path.isfile(self.get_path()):
				raise IOError(
					'FileResource: by default, I refuse to overwrite files. '
					+ self.get_path()
				)

			# check if the directory exists, if not make it
			if not os.path.exists(self.path):
				os.makedirs(self.path)

			# ensure that the directory is not an existing file
			if os.path.isfile(self.path):
				raise IOError(
					'FileResource: the given path corresponds to an existing '
					'*file*.'
				)

		# hands over a plain file handle
		return open(self.get_path(), flags)



class IncrementalFileResource(object):
	'''
		This is like a file resource, but we don't rely on the existence of
		the file to determine whether the resource is satisfied.  We use a
		separate "marker" file, that is created when the resource is marked
		as done (by calling mark_as_done()).  This file contains a timestamp
		of when the resource was marked done according to local machine's time
	'''

	def __init__(self, path, fname, part_name):
		self.path = path
		self.fname = fname
		self.part_name = part_name

		
	def exists(self):
		return os.path.isfile(get_marker_path)


	def get_marker_path(self):
		return self.get_path() + '.' + self.part_name + '.marker'


	def mark_as_done(self):
		# note that this will create a marker file with the current time
		# stamp.  In general, it should never be called if there is an existing
		# file already by the same name, however, if that does happen somehow
		# it will append to the file.
		marker_fh = open(self.get_marker_path(), 'a')
		marker_fh.write('%s\n' % str(datetime.now()))





