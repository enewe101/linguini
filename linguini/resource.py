import os
from datetime import datetime


class ResourceException(Exception):
	pass


class Resource(object):


	def __init__(self, **kwargs):
		self.resolve_static(**kwargs)
		self.resolve_inherited(**kwargs)


	def resolve_inherited(self, **kwargs):
		self.instance_lot = kwargs.pop('lot', None)
		self.instance_pilot = kwargs.pop('pilot', None)
		self.instance_clobber = kwargs.pop('clobber', None)


	def resolve_static(self, **kwargs):
		# Resolve the `static`, `share`, and `ignore_pilot` attributes
		self.static = kwargs.pop('static', False)
		self.share = kwargs.pop('share', False)
		self.ignore_pilot = kwargs.pop('ignore_pilot', False)


	def get_ready(self, lot, pilot, name, clobber):

		# resolve propogation of the run properties
		self.inherited_lot = lot
		self.inherited_pilot = pilot
		self.inherited_clobber = clobber
		self.name = name

		# make sure that the lot is string-like or None
		lot_is_not_string = not isinstance(self.get_lot(), basestring)
		lot_is_not_none = self.get_lot() is not None
		if lot_is_not_string and lot_is_not_none:
			raise ValueError(
				'`lot` must be an instance of basestr, like str or unicode.'
			)

		self._is_ready = True


	def is_ready(self):
		try:
			return self._is_ready
		except AttributeError:
			return False


	def get_clobber(self):
		if self.instance_clobber is not None:
			return self.instance_clobber

		if hasattr(self, 'clobber') and self.clobber is not None:
			return self.clobber

		if self.inherited_clobber is not None:
			return self.inherited_clobber

		raise ResourceException(
			'could not resolve clobber in %s' % self.__class__.__name__)


	def get_pilot(self):
		if self.ignore_pilot or self.static:
			return False

		if self.instance_pilot is not None:
			return self.instance_pilot

		if hasattr(self, 'pilot') and self.pilot is not None:
			return self.pilot

		if self.inherited_pilot is not None:
			return self.inherited_pilot

		raise ResourceException(
			'could not resolve pilot in %s' % self.__class__.__name__)


	def get_lot(self):
		if self.share or self.static:
			return None

		if self.instance_lot is not None:
			return self.instance_lot

		if hasattr(self, 'lot') and self.lot is not None:
			return self.lot

		if self.inherited_lot is not None:
			return self.inherited_lot

		return None



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
			(('%s_' % self.get_lot()) if self.get_lot() is not None else '')
			+ ('pilot_' if self.get_pilot() else '')
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
				if self.get_clobber():
					print '\t INFO: clobbered %s' % self.get_path()
				else:
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
			if self.get_clobber():
				print '\tINFO: clobbered %s' % self.get_fname(fname)

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

