from task import Task
from resource import Resource, File

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


	#def check_layout(self):
	#	# ensure that all tasks in layout are defined in tasks
	#	laid_out_tasks = set([t for t in self.layout if t is not 'END'])

	#	missing = [
	#		t for t in self.layout 
	#		if t not in self.tasks and t is not 'END'
	#	]
	#	if len(missing)>0:
	#		raise RunnerException(
	#			'The following tasks were defined in the layout, but are '
	#			'not defined in the tasks: %s' % ', '.join(missing)
	#		)

	#	unused = [t for t in self.tasks if t not in self.layout]
	#	if len(missing)>0:
	#		raise RunnerException(
	#			'Warning, the following tasks are defined, but not included '
	#			'in the layout (they will never be run): %s'
	#			% ', '.join(unused)
	#		)


	#def _until_names(self):
	#	'''
	#		Get the names of the last tasks to be run.  Default is the task(s) 
	#		marked in layout by the key 'END'; can be overridden by class 
	#		variable.
	#	'''
	#	return as_list(self.until or self.layout['END'])


	#def _until_tasks(self):
	#	'''
	#		Get the last tasks to be run.
	#	'''
	#	return [self.get_task(t) for t in self._until_names()]


	def get_all_outputs(self):
		'''
			Collect all the outputs from the tasks that end this runner.
			These are taken to be the outputs of the runner itself.
		'''
		return reduce(
			lambda x,y: x+self.get_task(y).get_all_outputs(),
			self.tasks.keys(),
			[]
		)

	def get_task(self, task_name):
		return as_list(self.tasks[task_name])[0]

	def get_ready(
			self,
			lot=None,
			as_pilot=False,
			name='main',
			clobber=False
		):

		if self.is_ready():
			return


		# sub-runners inherit their parent runners lot name here.
		# ordinary runners 'inherit' lot by keyword argument to run() here.
		# If runner instantiated with share=True, lot is not inherited
		if not self.share and lot is not None:
			self.lot = lot

		# if ignore pilot is true, then pilot is forced false.  Otherwise
		# it is inherited
		if self.ignore_pilot:
			self.as_pilot = False
		else:
			self.resolve_attr('as_pilot', as_pilot, False)

		self.resolve_attr('name', name)
		self.resolve_attr('clobber', clobber)

		# resolve tasks
		self.tasks = self._tasks()

		# get all the tasks ready
		for task_name in self.tasks:

			task = as_list(self.tasks[task_name])[0]

			task.get_ready(
				lot=self.lot, as_pilot=self.as_pilot, name=task_name, 
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
		scheduled = set()

		for task_name in reversed(task_names):

			task_def = as_list(self.tasks[task_name])
			task = task_def[0]
			dependencies = task_def[1:]

			if not task.exists():

				# add tasks to the schedule, without permitting duplicates
				scheduled.add(task_name)

				# schedule dependant jobs, if any
				try:
					scheduled |= (self.recursively_schedule(dependencies))
				except KeyError:
					pass
				
		return scheduled


	def check_schedule(self):

		checked_tasks = set()
		okay, problem = True, None
		for task_name in self.tasks:
			okay, problem = self.check_task(task_name, checked_tasks, [])
			if not okay:
				return okay, problem

		return okay, problem


	def check_task(self, task_name, checked_tasks, pending_tasks):

		okay = True
		pending_tasks.append(task_name)

		try:
			dependencies = as_list(self.tasks[task_name])[1:]
		except:
			return (
				False, 
				'%s is listed as a dependency but never defined in %s tasks'
				% (task_name, self.__class__.__name__)
			)

		# detect cyclical dependencies arising at this step
		cyclical_dependencies = [d for d in dependencies if d in pending_tasks]
		if len(cyclical_dependencies):
			cyc_d = cyclical_dependencies[0]
			entry = pending_tasks.index(cyc_d)
			cycle = pending_tasks[entry:] + [cyc_d]
			problem = 'cyclical dependency: %s' % [' -> '].join(cycle)
			return False, problem

		okay, problem = True, None
		for d in dependencies:

			# we don't need to re-check previously checked tasks
			if d in checked_tasks:
				continue

			# recurse, check this tasks dependencies
			okay, problem = self.check_task(d, checked_tasks, pending_tasks)
			if not okay:
				return okay, problem

		this_task_name = pending_tasks.pop()
		assert(this_task_name == task_name)
		checked_tasks.add(task_name)

		return okay, problem


	def run_schedule(self):

		# keep looping as long as their are incomplete tasks
		while len(self.schedule)>0:

			remaining = [t for t in self.schedule]

			# look at each task left in the schedule
			for task_name in remaining:

				# get the task definition
				task_def = as_list(self.tasks[task_name])
				task = task_def[0]
				dependencies = task_def[1:]

				# if the task has scheduled dependencies, we can't run it yet
				if any([d in self.schedule for d in dependencies]):
					continue

				# run the task, and remove it from the schedule
				task._run()
				self.schedule.remove(task_name)


	def _run(
			self, 
			lot=None,
			as_pilot=False,
			until=None,
			clobber=None,
			share=False
		):

		self.run(lot, as_pilot=as_pilot, until=until, clobber=clobber)


	def run(
			self, 
			lot=None,
			as_pilot=False,
			until=None,
			clobber=False,
			share=False
		):

		self.share = share

		self.get_ready(
			lot=lot, 
			as_pilot=as_pilot,
			name='main',
			clobber=clobber
		)

		if self.lot is None:
			raise RunnerException(
				'Runner has no lot: %s' % self.__class__.__name__)

		# resolve until
		if until is not None:
			self.until = until

		elif not hasattr(self, 'until'):
			self.until = None

		elif hasattr(self, '_until'):
			self.until = self._until()

		#print 'Starting runner for lot %s.' % self.lot

		# by default, run whatever is in 'END'
		until = until or self.until
		if until is None:
			until = self.tasks.keys()

		# ensure that the schedule can actually complete
		okay, problem = self.check_schedule()
		if not okay:
			raise RunnerException(problem)

		# schedule the necessary tasks
		self.schedule = self.recursively_schedule(until)

		# run the tasks
		self.run_schedule()


