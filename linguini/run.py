from task import Task
from resource import Resource, FileResource

def as_list(item):
	if isinstance(item, dict):
		return item.values()

	if hasattr(item, '__iter__'):
		return list(item)

	return [item]


class RunnerException(Exception):
	pass


class Runner(Task):

	# The null runner has no tasks, but is valid
	tasks = {}
	layout = {
		'END':[]
	}
	until = None
	lot = None


	# setting these here makes the call to super.get_ready work, after which
	# inputs and outputs are set to the source ind sink tasks' inputs and 
	# outputs.  Any values actually set to these class variables would be 
	# ignored
	outputs = None
	inputs = None

	def _tasks(self):
		return self.tasks

	def _layout(self):
		return self.layout

	def _until(self):
		return self.until


	def check_layout(self):
		# ensure that all tasks in layout are defined in tasks
		laid_out_tasks = set([t for t in self.layout if t is not 'END'])

		missing = [
			t for t in self.layout 
			if t not in self.tasks and t is not 'END'
		]
		if len(missing)>0:
			raise RunnerException(
				'The following tasks were defined in the layout, but are '
				'not defined in the tasks: %s' % ', '.join(missing)
			)

		unused = [t for t in self.tasks if t not in self.layout]
		if len(missing)>0:
			raise RunnerException(
				'Warning, the following tasks are defined, but not included '
				'in the layout (they will never be run): %s'
				% ', '.join(unused)
			)


	def _until_names(self):
		'''
			Get the names of the last tasks to be run.  Default is the task(s) 
			marked in layout by the key 'END'; can be overridden by class 
			variable.
		'''
		return as_list(self.until or self.layout['END'])


	def _until_tasks(self):
		'''
			Get the last tasks to be run.
		'''
		return [self.tasks[t] for t in self._until_names()]


	def get_all_outputs(self):
		'''
			Collect all the outputs from the tasks that end this runner.
			These are taken to be the outputs of the runner itself.
		'''
		return reduce(
			lambda x,y: x+y.get_all_outputs(),
			self._until_tasks(),
			[]
		)


	def get_ready(
			self,
			lot=None,
			as_pilot=False,
			name='main',
			clobber=False
		):

		if self.is_ready():
			return

		# unlike other tasks, by default, runs do *not* inherit lot from parent
		# runs, unless explicitly done at object instantiation.
		# note that, at the time that get_ready is run, any lot value set 
		# during instantiation will have already been set to self.lot
		if not hasattr(self, 'lot') or self.lot is None:
			raise RunnerException('No lot was given!')

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

		# resolve tasks
		self.tasks = self._tasks()

		# get all the tasks ready
		for task_name, task in self.tasks.iteritems():
			task.get_ready(
				lot=self.lot, as_pilot=as_pilot, name=task_name, 
				clobber=clobber
			)

		# resolve layout
		self.layout = self._layout()

		# it isn't necessary to specify a run's outputs, because they are
		# taken to be those of the END tasks
		self.outputs = self.get_all_outputs()

		# mark as ready
		self._is_ready = True


	def recursively_schedule(self, task_names):
		'''
			accepts the name of a task, or an iterable thereof, and schedules
			those which are not yet done.
		'''
		task_names = as_list(task_names)
		scheduled = []
		for task_name in reversed(task_names):

			if not self.tasks[task_name].exists():
				scheduled.append(task_name)

				# schedule dependant jobs, if any
				try:
					scheduled.extend(
						self.recursively_schedule(self.layout[task_name])
					)
				except KeyError:
					pass
				
		return scheduled


	def _run(self, lot=None, as_pilot=False, until=None, clobber=None):

		self.run(lot, as_pilot=as_pilot, until=until, clobber=clobber)


	def run(self, lot=None, as_pilot=None, until=None, clobber=False):

		self.get_ready(lot, as_pilot, 'main', clobber)

		# resolve until
		if until is not None:
			self.until = until

		elif not hasattr(self, 'until'):
			self.until = None

		elif hasattr(self, '_until'):
			self.until = self._until()

		# make sure that all the tasks are well-defined and actually get used
		self.check_layout()

		#print 'Starting runner for lot %s.' % self.lot

		# by default, run whatever is in 'END'
		until = until or self.until
		if until is None:
			until = self.layout['END']

		schedule = self.recursively_schedule(until)

		# completely run the until_tasks
		while len(schedule) > 0:
			next_task_name = schedule.pop()
			#print ' - running %s.' % next_task_name
			self.tasks[next_task_name]._run()


