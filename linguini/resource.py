import os
from datetime import datetime


class ResourceException(Exception):
	pass


class Resource(object):


	def __init__(self, **kwargs):
		self.resolve_static(**kwargs)


	def resolve_static(self, **kwargs):
		# Resolve the `static`, `share`, and `ignore_pilot` attributes
		self.static = kwargs.pop('static', False)
		self.share = kwargs.pop('share', False)
		self.ignore_pilot = kwargs.pop('ignore_pilot', False)

		if self.static:
			self.share = True
			self.ignore_pilot = True


	def resolve_attr(self, attr_name, attr_val, default=None, raises=True):

		# self.attr_name takes precedence, but if unset or none, use attr_val 
		if not hasattr(self, attr_name) or getattr(self, attr_name) is None:
			setattr(self, attr_name, attr_val)

		# if attr_val is None, try using the default value
		if getattr(self, attr_name) is None:
			setattr(self, attr_name, default)

		# if the attribute is still None, maybe raise exception
		if getattr(self, attr_name) is None and raises:
			raise ResourceException('No value found for %s.' % attr_name)

		# attribute value was set to self.attr_name, but we also return it
		return getattr(self, attr_name)

		
	def get_ready(self, lot, as_pilot, name, clobber):

		# resolve propogation of the run properties
		#
		# lot can be none now, because of how static / share works
		self.resolve_attr('lot', lot, raises=False)
		self.resolve_attr('as_pilot', as_pilot, False)

		self.resolve_attr('name', name)
		self.resolve_attr('clobber', clobber)

		# enforce `ignore_pilot` and `share`, by overriding lot and pilot
		if self.ignore_pilot:
			self.as_pilot = False

		if self.share:
			self.lot = None

		# make sure that the lot is string-like or None
		if not isinstance(self.lot, basestring) and self.lot is not None :
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


class File(Resource):

	def __init__(self, path, fname, **kwargs):
		self.path = path
		self.fname = fname
		super(File, self).__init__(**kwargs)


	def get_path(self):

		# ensure the resource is ready
		if not self.is_ready():
			raise ResourceException('Resource is not ready.')

		fname = (
			(('%s_' % self.lot) if self.lot is not None else '')
			+ ('pilot_' if self.as_pilot else '')
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
					'File: by default, I refuse to overwrite files. '
					+ self.get_path()
				)

			# check if the directory exists, if not make it
			if not os.path.exists(self.path):
				os.makedirs(self.path)

			# ensure that the directory is not an existing file
			if os.path.isfile(self.path):
				raise IOError(
					'File: the given path corresponds to an existing '
					'*file*.'
				)

		# hands over a plain file handle
		return open(self.get_path(), flags)


class MarkerResource(File):
	def mark(self):

		# if the marker path already exists, perfect
		if os.path.isdir(self.path):
			pass

		# if the marker path points to a file, not a dir, fail
		elif os.path.exists(self.path):
			raise IOError(
				('MarkerResource: I could not mark task `%s` as done because '
				'there is a file in the place of the directory I am supposed '
				'to write to') % self.name
			)

		# if the path doesn't exist yet, make it
		else:
			os.makedirs(self.path)

		self.open('a').write('%s\n' % str(datetime.now()))


class IncrementalFile(object):
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




class Folder(File):
	'''
		Creates (if necessary) a folder that is namepsaced to the lot, 
		and allows reading and writing files there.  File names are not
		namespaced (because the folder is).
	'''

	def get_fname(self, fname):
		return os.path.join(self.get_path(), fname)

	def open(self, fname, mode):

		# check if the folder exists yet, if yes, move on
		if os.path.isdir(self.get_path()):
			pass

		# if a file (rather than folder) exists, it's an error
		elif os.path.exists(self.get_path()):
			raise IOError(
				'Folder: the given path corresponds to an existing '
				'*file*.  I cannot create a directory here: %s' 
				% self.get_path()
			)

		# if not, make the folder
		else:
			os.mkdir(self.get_path())

		# check if the specific file exists (as a folder or file)
		if os.path.isdir(self.get_fname(fname)):
			raise IOError(
				'Folder: a file or folder exists there. '
				'I can\'t make a file: %s' % self.get_fname(fname)
			)

		# if we're opening in write mode, don't overwrite an existing file
		if 'w' in mode and os.path.isfile(self.get_fname(fname)):
			raise IOError(
				'Folder: a file already exists there. '
				'I don not overwrite by default: %s' % self.get_fname(fname)
			)

		# if all is good, open and yield the file resource
		return open(self.get_fname(fname), mode)


	#TODO: define exists
	def exists(self):
		pass

