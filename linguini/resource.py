import os
from datetime import datetime
from utils import saves_args


class ResourceException(Exception):
	pass


class Resource(object):


	@saves_args
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
			raise ValueError('`lot` must be string-like.')

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


	def copy(self):
		return self.__class__(*self.args['args'], **self.args['kwargs'])


class File(Resource):

	@saves_args
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


	def get_dir(self):
		return os.path.dirname(self.get_path())


	def exists(self):
		return os.path.isfile(self.get_path())

	def open(self, flags):

		# avoid clobbering files (if that's not what we're told to do)
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

		# if we're opining in a write mode, then make dirs if needed
		if 'a' in flags or 'w' in flags:
			if not os.path.isdir(self.get_path()):
				os.makedirs(self.get_path())

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

	@saves_args
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

	@saves_args
	def __init__(self, path, dirname, *args, **kwargs):
		self.whitelist = kwargs.pop('whitelist', None)
		self.blacklist = kwargs.pop('blacklist', None)
		super(Folder, self).__init__(path, dirname, *args, **kwargs)

	def get_fname(self, fname):
		return os.path.join(self.get_path(), fname)


	def prepare_to_open(self, fname, mode):

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
				'Folder: a folder exists there. '
				'I can\'t make a file, even if clobber is True: %s' 
				% self.get_fname(fname)
			)

		# if we're opening in write mode, don't overwrite an existing file
		# unless in clobber mode
		if 'w' in mode and os.path.isfile(self.get_fname(fname)):

			if self.get_clobber():
				print '\tINFO: clobbered %s' % self.get_fname(fname)

			else:
				raise IOError(
					'Folder: a file already exists there. '
					'I do not overwrite by default: %s' % self.get_fname(fname)
				)


	def open(self, fname, mode):
		self.prepare_to_open(fname, mode)
		return open(self.get_fname(fname), mode)


	def __iter__(self):
		# make a fresh list of all the files in the dir
		files = [
			os.path.abspath(os.path.join(self.get_path(), f))
			for f in os.listdir(self.get_path()) 
		]
		self.files = [f for f in files if os.path.isfile(f)]
		return self


	def next(self):

		try:
			fname = self.files.pop()
		except IndexError:
			raise StopIteration

		if self.whitelist is not None:
			if self.whitelist.match(fname):
				return fname
			else:
				return self.next()

		if self.blacklist is not None:
			if self.blacklist.match(fname):
				return self.next()
			else:
				return fname

		return fname


	#TODO: define exists
	def exists(self):
		pass

