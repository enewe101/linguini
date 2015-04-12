import os
from datetime import datetime


class ResourceException(Exception):
	pass


class Resource(object):


	def resolve_attr(self, attr_name, attr_val, default=None, raises=True):

		if not hasattr(self, attr_name) or getattr(self, attr_name) is None:
			setattr(self, attr_name, attr_val)

		if getattr(self, attr_name) is None:
			setattr(self, attr_name, default)

		if getattr(self, attr_name) is None and raises:
			raise ResourceException('No value found for %s.' % attr_name)

		return getattr(self, attr_name)



		
	def get_ready(self, lot, as_pilot, name, clobber):

		# resolve propogation of the run properties
		self.resolve_attr('lot', lot)
		self.resolve_attr('as_pilot', as_pilot, False)
		self.resolve_attr('name', name)
		self.resolve_attr('clobber', clobber)

		# make sure that the lot is string-like
		if not isinstance(self.lot, basestring):
			raise ValueError(
				'`lot` must be an instance of basestr, like str or unicode.'
			)

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


class MarkerResource(FileResource):
	def mark(self):
		self.open('a').write('%s\n' % str(datetime.now()))


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




class FolderResource(FileResource):
	'''
		Creates (if necessary) a folder that is namepsaced to the lot, 
		and allows reading and writing files there.  File names are not
		namespaced (because the folder is).
	'''

	def get_fname(self):
		return os.path.join(self.get_path(), fname)

	def open(self, fname, mode):

		# check if the folder exists yet, make it if not 
		if os.isdir(self.get_path()):
			pass

		# if a file (rather than folder) exists, it's an error
		elif os.exists(self.get_path()):
			raise IOError(
				'FolderResource: the given path corresponds to an existing '
				'*file*.  I cannot create a directory here: %s' 
				% self.get_path()
			)

		# if not, make the folder
		else:
			os.mkdir(self.get_path())

		# check if the specific file exists (as a folder or file)
		if os.isdir(self.get_fname()):
			raise IOError(
				'FolderResource: a file or folder exists there. '
				'I can\'t make a file: %s' % self.get_fname()
			)

		# if we're opening in write mode, don't overwrite an existing file
		if 'w' in mode and os.isfile(self.get_fname()):
			raise IOError(
				'FolderResource: a file already exists there. '
				'I don not overwrite by default: %s' % self.get_fname()
			)

		# if all is good, open and yield the file resource
		return open(self.get_fname(), mode)


	#TODO: defined exists
	def exists(self):
		pass

