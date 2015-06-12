import inspect
from functools import wraps


def copy(obj):
	if isinstance(obj, dict):
		new_obj = dict([(k, v.copy()) for k,v in obj.items()])

	elif isinstance(obj, (tuple, list)):
		new_obj = obj.__class__([v.copy() for v in obj])

	elif obj is None:
		new_obj = obj

	else:
		new_obj = obj.copy()

	return new_obj


def saves_args(f):
	@wraps(f)
	def wrapped(self, *args, **kwargs):
		if not hasattr(self, 'args'):
			argspec = inspect.getargspec(f)
			_args = locals()['args']
			_kwargs = locals()['kwargs']
			self.args = {'args': _args, 'kwargs': _kwargs}

		return f(self, *args, **kwargs)

	return wrapped






