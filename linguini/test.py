import shutil
import unittest
from unittest import TestCase
from run import Runner, RunnerException
from task import Task, TaskException, MarkedTask, SimpleTask
from resource import Resource, File, Folder
import os


TEST_DIR = 'temporary_testing_files'

#TODO: test that the file resource creates folders along the path if needed
#	when opened in 'a' or 'w' modes

def touch(fname):
	open(fname, 'a').close()


class TestFolderResource(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)
		shutil.rmtree('./linguini_markers')

	def test_basic_write(self):

		dir_name = 'foldir'
		file_name = 'fyle.txt'
		lot_name = 'my_lot'

		class MyTask(SimpleTask):
			outputs = Folder(TEST_DIR, dir_name)
			was_run = False
			def run(self):
				self.was_run = True
				self.outputs.open(file_name, 'w').write('yo')

		class MyRunner(Runner):
			lot = lot_name
			tasks = {
				'task': MyTask()
			}

		MyRunner().run()
		expected_dir = os.path.join(TEST_DIR, '%s_%s' % (lot_name, dir_name))
		expected_file = os.path.join(expected_dir, file_name)
		self.assertTrue(os.path.isdir(expected_dir))
		self.assertTrue(os.path.isfile(expected_file))


class TestRunner(TestCase):

	def test_null_runner(self):

		# if you don't define a lot, an exception is raised
		runner = Runner()
		with self.assertRaises(RunnerException):
			runner.run()

		# A null runner can be run, once you specify the lot
		class MyRunner(Runner):
			lot = 'my_lot'

		runner = MyRunner()
		runner.run()


	def test_run_until(self):

		class MyTask(Task):
			outputs = None
			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False
			def run(self):
				self.was_run = True

		task0 = MyTask()
		task1 = MyTask()
		task2 = MyTask()
		task3 = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2': (task2, 'task1', 'task0'),
				'task3': (task3, 'task2')
			}

		my_runner = MyRunner()
		my_runner.run(until='task2')

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertFalse(task3.was_run)


	def test_run_until_classdef(self):

		class MyTask(Task):
			outputs = None
			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False
			def run(self):
				self.was_run = True

		task0 = MyTask()
		task1 = MyTask()
		task2 = MyTask()
		task3 = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			until = 'task2'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2': (task2, 'task1', 'task0'),
				'task3': (task3, 'task2')
			}

		my_runner = MyRunner()
		my_runner.run()

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertFalse(task3.was_run)


	def test_run_until_end(self):

		class MyTask(Task):
			outputs = None
			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False
			def run(self):
				self.was_run = True

		task0 = MyTask()
		task1 = MyTask()
		task2 = MyTask()
		task3 = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2': (task2, 'task1', 'task0'),
				'task3': (task3, 'task2')
			}

		my_runner = MyRunner()
		my_runner.run()

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertTrue(task3.was_run)

	
	def test_catch_cyclical_dependency(self):

		class MyTask(SimpleTask):
			def run(self):
				pass

		# a simple cycle
		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {
				'task0': (MyTask(), 'task1'),
				'task1': (MyTask(), 'task0'),
			}

		with self.assertRaises(RunnerException):
			MyRunner().run()

		# check that the cyclical dependency is reported correctly
		okay, problem = MyRunner().check_schedule()
		self.assertEqual(
			problem,
			'cyclical dependency: task0 -> task1 -> task0'
		)



		# a bit more complicated cycle
		class MoRunner(Runner):
			lot = 'mo_lot'
			tasks = {
				'task0': (MyTask(), 'task1', 'task2'),
				'task1': (MyTask(), 'task3', 'task4'),
				'task2': (MyTask(), 'task5', 'task6'),
				'task3': (MyTask(), 'task2'),
				'task4': MyTask(),
				'task5': (MyTask(), 'task1'),
				'task6': MyTask()
			}
			
		with self.assertRaises(RunnerException):
			MoRunner().run()

		# check that the cyclical dependency is reported correctly
		okay, problem = MoRunner().check_schedule()
		self.assertEqual(
			problem,
			'cyclical dependency: task1 -> task3 -> task2 -> task5 -> task1'
		)

		# here we point to a potentially previously scheduled task, but 
		# there's no cycle -- it should work
		class MiRunner(Runner):
			lot = 'mo_lot'
			tasks = {
				'task0': (MyTask(), 'task1', 'task2'),
				'task1': (MyTask(), 'task3', 'task4'),
				'task2': (MyTask(), 'task5', 'task6'),
				'task3': (MyTask(), 'task2'),
				'task4': MyTask(),
				'task5': (MyTask(), 'task6'),
				'task6': MyTask()
			}
			
		MiRunner().run()

	def test_catch_nonexistent_dependency(self):

		class MyTask(SimpleTask):
			def run(self):
				pass

		# task 1 is not defined!
		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {
				'task0': (MyTask(), 'task1'),
			}

		with self.assertRaises(RunnerException):
			MyRunner().run()

		# task7 is not defined!
		class MoRunner(Runner):
			lot = 'mo_lot'
			tasks = {
				'task0': (MyTask(), 'task1', 'task2'),
				'task1': (MyTask(), 'task3', 'task4'),
				'task2': (MyTask(), 'task5', 'task6', 'task7'),
				'task3': (MyTask(), 'task2'),
				'task4': MyTask(),
				'task5': (MyTask(), 'task6'),
				'task6': MyTask()
			}
			
		with self.assertRaises(RunnerException):
			MoRunner().run()

class TestMarkerTask(TestCase):
	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)

	def test_mark_marks(self):

		
		class MyTask(MarkedTask):
			marker_path = TEST_DIR
			outputs = None
			num_runs = 0
			def run(self):
				self.num_runs += 1

		task1 = MyTask()
		task_name = 'task1'
		lot_name = 'my_lot'

		class MyRunner(Runner):
			lot = lot_name
			tasks = {
				task_name: task1
			}

		my_runner = MyRunner()

		# run the runner
		my_runner.run()
		# check that the task was run
		self.assertTrue(task1.num_runs == 1)

		# run again
		my_runner.run()
		# this task should have been skipped due to existence of marker file
		self.assertTrue(task1.num_runs == 1)

		# remove the marker file
		os.remove(
			os.path.join(TEST_DIR, '%s_%s.marker' % (lot_name, task_name))
		)

		# run again
		my_runner.run()
		# the task should run again
		self.assertTrue(task1.num_runs == 2)


	def test_marker_as_mixin(self):
		'''
			this test is similar to the last, but it tests a situation where
			a pre-existing task is augmented using the mixin approach.
			rather than MyTask inheriting directly from MarkedTask,
			MyTask is defined on top of Task, and then an augmented version
			is made afterward, MyMarkedTask, by using MarkedTask as a mixin.

			This tests all the conditions in test_marker_marks, but it also
			tests that if MyTask overrides a method in Task, augmenting 
			with MarkedTask as a mixin preserves that.

			Of course, the `exists()` function is not preserved -- changing 
			that is the whole reason for using the MarkedTask as a mixin.
		'''
		
		class MyTask(Task):
			ready_override = False
			after_override = False
			def get_ready(self, lot, pilot, name, clobber):
				super(MyTask, self).get_ready(
					lot, pilot, name, clobber
				)
				self.ready_override = True

			def _after(self):
				super(MyTask, self)._after()
				self.after_override = True


		class MyMarkedTask(MarkedTask, MyTask):
			marker_path = TEST_DIR
			outputs = None
			num_runs = 0
			def run(self):
				self.num_runs += 1

		task1 = MyMarkedTask()
		task_name = 'task1'
		lot_name = 'my_lot'

		class MyRunner(Runner):
			lot = lot_name
			tasks = {
				task_name: task1
			}

		my_runner = MyRunner()

		# run the runner
		my_runner.run()
		# check that the task was run
		self.assertTrue(task1.num_runs == 1)

		# check that the version of get_ready from MyTask was run
		self.assertTrue(task1.ready_override)
		# check that the version of after from MyTask was run
		self.assertTrue(task1.after_override)

		# run again
		my_runner.run()
		# this task should have been skipped due to existence of marker file
		self.assertTrue(task1.num_runs == 1)

		# remove the marker file
		os.remove(
			os.path.join(TEST_DIR, '%s_%s.marker' % (lot_name, task_name))
		)

		# run again
		my_runner.run()
		# the task should run again
		self.assertTrue(task1.num_runs == 2)


class TestSimpleTask(TestCase):

	TEST_DIR = 'linguini_markers'
	def tearDown(self):
		shutil.rmtree(self.TEST_DIR)

	def test_simple_task(self):
		'''
			Simple task is based on marked task, and provides the simplest
			interface for turning some piece of functionality into a linguini
			task.  One simply defines the run function, that's all
		'''

		class MyTask(SimpleTask):
			num_runs = 0
			def run(self):
				self.num_runs += 1

		task1 = MyTask()
		task_name = 'task1'
		lot_name = 'my_lot'

		class MyRunner(Runner):
			lot = lot_name
			tasks = {
				task_name: task1
			}

		my_runner = MyRunner()

		# run the runner
		my_runner.run()
		# check that the task was run
		self.assertTrue(task1.num_runs == 1)

		# run again
		my_runner.run()
		# this task should have been skipped due to existence of marker file
		self.assertTrue(task1.num_runs == 1)

		# remove the marker file
		os.remove(
			os.path.join(self.TEST_DIR, '%s_%s.marker' % (lot_name, task_name))
		)

		# run again
		my_runner.run()
		# the task should run again
		self.assertTrue(task1.num_runs == 2)



class TestCompoundRunner(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)

	def test_run_compound(self):
		'''
			Tests that a compound runner does run the entire thing
		'''
		
		class MyTask(Task):

			def _outputs(self):
				fname = 'test%d.txt' % self.parameters['file_num']
				return File(TEST_DIR, fname)

			was_run = False		# Track whether the task was run
			def exists(self):	# ensures the task will not be skipped
				return False

			def run(self):
				fh = self.outputs.open('w')
				fh.write('yo')
				fh.close()
				self.was_run = True

		task0 = MyTask(file_num=0)
		task1 = MyTask(file_num=1)
		task2 = MyTask(file_num=2)
		task3 = MyTask(file_num=3)
		task4 = MyTask(file_num=4)
		task5 = MyTask(file_num=5)
		task6 = MyTask(file_num=6)
		task7 = MyTask(file_num=7)

		class MyRunner(Runner):
			outputs = None
			lot = 'a'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2': (task2, 'task1', 'task0'),
				'task3': (task3, 'task2')
			}
			def exists(self):
				return False

		class MoRunner(Runner):
			outputs = None
			lot = 'b'
			tasks = {
				'task0':task4,
				'task1':task5,
				'task2': (task6, 'task1', 'task0'),
				'task3': (task7, 'task2')
			}
			def exists(self):
				return False

		class MasterRunner(Runner):
			lot = '1'
			tasks = {
				'run0': MyRunner(),
				'run1': (MoRunner(), 'run0'),
			}

		my_runner = MasterRunner()
		my_runner.run()

		self.assertTrue(task0.was_run)
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertTrue(task3.was_run)
		self.assertTrue(task4.was_run)
		self.assertTrue(task5.was_run)
		self.assertTrue(task6.was_run)
		self.assertTrue(task7.was_run)

		# the lot of master runner is inherited by both sub-runners
		expected_files = ['1_test%d.txt' % d for d in range(8)]
		self.assertItemsEqual(expected_files, os.listdir(TEST_DIR))


	def test_run_compound_partially_complete(self):
		'''
			Tests that a compound runner does runs the entire but checks
			whether the components have been run before
		'''
		
		class MyTask(Task):

			def _outputs(self):
				fname = 'test%d.txt' % self.parameters['file_num']
				return File(TEST_DIR, fname)

			was_run = False		# Track whether the task was run

			def run(self):
				fh = self.outputs.open('w')
				fh.write('yo')
				fh.close()
				self.was_run = True

		task0 = MyTask(file_num=0)
		task1 = MyTask(file_num=1)
		task2 = MyTask(file_num=2)
		task3 = MyTask(file_num=3)
		task4 = MyTask(file_num=4)
		task5 = MyTask(file_num=5)
		task6 = MyTask(file_num=6)
		task7 = MyTask(file_num=7)

		class MyRunner(Runner):
			outputs = None
			lot = 'a'
			tasks = {
				'task0':task0,
				'task1':task1,
				'task2': (task2, 'task1', 'task0'),
				'task3': (task3, 'task2')
			}
			def exists(self):
				return False

		class MoRunner(Runner):
			outputs = None
			lot = 'b'
			tasks = {
				'task0':task4,
				'task1':task5,
				'task2': (task6, 'task1', 'task0'),
				'task3': (task7, 'task2')
			}
			def exists(self):
				return False

		my_runner = MyRunner()
		mo_runner = MoRunner()


		class MasterRunner(Runner):
			lot = '1'
			tasks = {
				'run0': my_runner,
				'run1': (mo_runner, 'run0')
			}

		# make some resources so they won't be run
		touch(os.path.join(TEST_DIR, '1_test0.txt'))
		touch(os.path.join(TEST_DIR, '1_test6.txt'))

		my_runner = MasterRunner()
		my_runner.run()

		# these tasks should not have been run
		self.assertFalse(task0.was_run)
		self.assertFalse(task6.was_run)

		# these tasks should have been run
		self.assertTrue(task1.was_run)
		self.assertTrue(task2.was_run)
		self.assertTrue(task3.was_run)
		self.assertTrue(task4.was_run)
		self.assertTrue(task5.was_run)
		self.assertTrue(task7.was_run)

		# all the files should exist at this point
		expected_files = ['1_test%d.txt' % d for d in range(8)]

		# note, b_test4.txt and b_test5.txt never get made because
		# they aren't needed.

		self.assertItemsEqual(expected_files, os.listdir(TEST_DIR))





class TestResource(TestCase):
	

	def test_exists_not_implemented(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			pass

		class MyTask(Task):
			outputs = MyResource()

			def run(self):
				print 'hello world'

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}

		runner = MyRunner()
		with self.assertRaises(NotImplementedError):
			runner.run()


	def test_exists_implemented(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			def exists(self):
				return True

		class MyTask(Task):
			outputs = MyResource()

			def run(self):
				print 'hello world'

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}

		runner = MyRunner()
		runner.run()



class TestTask(TestCase):

	def test_no_output(self):	

		# a task has to have some outputs
		class MyTask(Task):
			def run(self):
				print 'hello world'

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}

		runner = MyRunner()

		with self.assertRaises(TaskException):
			runner.run()


	def test_run_not_implemented(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = Resource()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':MyTask()}

		runner = MyRunner()

		with self.assertRaises(NotImplementedError):
			runner.run()


	def test_simple_task_gets_run(self):
		'''
			Test that a task that doesn't define a run function raises a 
			not implemented error
		'''
		
		class MyResource(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = MyResource()
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}

		runner = MyRunner()
		runner.run()

		self.assertTrue(my_task.was_run)


	def test_simple_task_not_run(self):
		'''
			Test that a task whose only resource already exists does not get 
			run.
		'''
		
		class MyResource(Resource):
			def exists(self):
				return True

		class MyTask(Task):
			outputs = MyResource()
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}

		runner = MyRunner()
		runner.run()

		self.assertFalse(my_task.was_run)



	def test_simple_task_many_resources_dict_run(self):
		'''
			Test that a task with many resources as a list,
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True

		class Noexists(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = {
				'one': Exists(),
				'two': Noexists(),
				'three': Exists(),
				'four': Exists()
			}
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}

		runner = MyRunner()
		runner.run()

		self.assertTrue(my_task.was_run)


	def test_simple_task_many_resources_dict_no_run(self):
		'''
			Test that a task with many resources as a list, not 
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True

		class MyTask(Task):
			outputs = {
				'one': Exists(),
				'two': Exists(),
				'three': Exists(),
				'four': Exists()
			}
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}

		runner = MyRunner()
		runner.run()

		self.assertFalse(my_task.was_run)


	def test_simple_task_many_resources_no_run(self):
		'''
			Test that a task with many resources as a list, not 
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True


		class MyTask(Task):
			outputs = (Exists(), Exists(), Exists(), Exists())
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}

		runner = MyRunner()
		runner.run()

		self.assertFalse(my_task.was_run)




	def test_simple_task_many_resources(self):
		'''
			Test that a task with many resources as a list, not 
			gets run if at least one of them does not exist
		'''
		
		class Exists(Resource):
			def exists(self):
				return True

		class Noexists(Resource):
			def exists(self):
				return False

		class MyTask(Task):
			outputs = (Exists(), Exists(), Noexists(), Exists())
			was_run = False

			def run(self):
				self.was_run = True

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_lot'
			tasks = {'my_task':my_task}

		runner = MyRunner()
		runner.run()

		self.assertTrue(my_task.was_run)


class TestStatic(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)
		if os.path.exists('./linguini_markers'):
			shutil.rmtree('./linguini_markers')


	def test_ignore_pilot_resource(self):
		'''
			Tests that, when a resource is invoked with the keyword argument
			ignore_pilot=True, it will not put 'pilot_' in front of its 
			file names.
		'''

		LOT_NAME = 'my_lot'

		# make a files to be read, one prepends pilot, the other doesn't
		open(os.path.join(TEST_DIR, '%s_in_A.txt'%LOT_NAME), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTask(SimpleTask):
			outputs = {
				'A': File(TEST_DIR, 'out_A.txt', ignore_pilot=True),
				'B': File(TEST_DIR, 'out_B.txt'),
			}
			inputs = {
				'A': File(TEST_DIR, 'in_A.txt', ignore_pilot=True),
				'B': File(TEST_DIR, 'in_B.txt')
			}

			def run(self):
				self.inputs['A'].open('r').read()
				self.inputs['B'].open('r').read()
				self.outputs['A'].open('w').write('yo')
				self.outputs['B'].open('w').write('yo')

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {'my_task':MyTask()}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_out_A.txt'%LOT_NAME)))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))


	def test_share_resource(self):
		'''
			Tests that, when a resource is invoked with the keyword argument
			share=True, it will not prepend the lot name in_front of its
			file names.
		'''

		LOT_NAME = 'my_lot'

		# make a files to be read, one prepends pilot, the other doesn't
		open(os.path.join(TEST_DIR, 'in_A.txt'), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTask(SimpleTask):
			outputs = {
				'A': File(TEST_DIR, 'out_A.txt', share=True),
				'B': File(TEST_DIR, 'out_B.txt'),
			}
			inputs = {
				'A': File(TEST_DIR, 'in_A.txt', share=True),
				'B': File(TEST_DIR, 'in_B.txt')
			}

			def run(self):
				self.inputs['A'].open('r').read()
				self.inputs['B'].open('r').read()
				self.outputs['A'].open('w').write('yo')
				self.outputs['B'].open('w').write('yo')

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {'my_task':MyTask()}

		# this will cause an error if the input prepended the input file name
		MyRunner().run()

		# check that the output was also not prepended by pilot
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, 'out_A.txt')))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_out_B.txt'%LOT_NAME)))


	def test_static_resource(self):
		'''
			Tests that, when a resource is invoked with the keyword argument
			static=True, it will not put the lot name nor 'pilot_' in front 
			of its file names.
		'''

		LOT_NAME = 'my_lot'

		# make a files to be read, one prepends pilot, the other doesn't
		open(os.path.join(TEST_DIR, 'in_A.txt'), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTask(SimpleTask):
			outputs = {
				'A': File(TEST_DIR, 'out_A.txt', static=True),
				'B': File(TEST_DIR, 'out_B.txt'),
			}
			inputs = {
				'A': File(TEST_DIR, 'in_A.txt', static=True),
				'B': File(TEST_DIR, 'in_B.txt')
			}

			def run(self):
				self.inputs['A'].open('r').read()
				self.inputs['B'].open('r').read()
				self.outputs['A'].open('w').write('yo')
				self.outputs['B'].open('w').write('yo')

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {'my_task':MyTask()}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, 'out_A.txt')))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))


class TestStaticTask(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)
		if os.path.exists('./linguini_markers'):
			shutil.rmtree('./linguini_markers')


	def test_ignore_pilot_task(self):
		'''
			Tests that, when a task is invoked with the keyword argument
			ignore_pilot=True, it will invoke it's resources with pilot=False.
			The resources will therefore not have their filenames prepended by
			'pilot_'.
		'''

		LOT_NAME = 'my_lot'

		# make a files to be read, one prepends pilot, the other doesn't
		open(os.path.join(TEST_DIR, '%s_in_A.txt'%LOT_NAME), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTaskA(SimpleTask):
			outputs = File(TEST_DIR, 'out_A.txt')
			inputs = File(TEST_DIR, 'in_A.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')


		class MyTaskB(SimpleTask):
			outputs = File(TEST_DIR, 'out_B.txt')
			inputs = File(TEST_DIR, 'in_B.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')


		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'my_static_task':MyTaskA(ignore_pilot=True),
				'my_task':MyTaskB()
			}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_out_A.txt'%LOT_NAME)))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))


	def test_share_task(self):
		'''
			Tests that, when a task is invoked with the keyword argument
			share=True, it will invoke it's resources with lot=None.  The 
			resources will therefore not have their filenames prepended by
			the lot.
		'''

		LOT_NAME = 'my_lot'

		# make a files to be read, one prepends pilot, the other doesn't
		open(os.path.join(TEST_DIR, 'pilot_in_A.txt'), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTaskA(SimpleTask):
			outputs = File(TEST_DIR, 'out_A.txt')
			inputs = File(TEST_DIR, 'in_A.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')


		class MyTaskB(SimpleTask):
			outputs = File(TEST_DIR, 'out_B.txt')
			inputs = File(TEST_DIR, 'in_B.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')


		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'my_static_task':MyTaskA(share=True),
				'my_task':MyTaskB()
			}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, 'pilot_out_A.txt')))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))


	def test_static_task(self):
		'''
			Tests that, when a task is invoked with the keyword argument
			static=True, it will invoke it's resources with lot=None and
			pilot=False.  The resources will therefore not have their
			filenames prepended.
		'''

		LOT_NAME = 'my_lot'

		# make a files to be read, one prepends pilot, the other doesn't
		open(os.path.join(TEST_DIR, 'in_A.txt'), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTaskA(SimpleTask):
			outputs = File(TEST_DIR, 'out_A.txt')
			inputs = File(TEST_DIR, 'in_A.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')


		class MyTaskB(SimpleTask):
			outputs = File(TEST_DIR, 'out_B.txt')
			inputs = File(TEST_DIR, 'in_B.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')


		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'my_static_task':MyTaskA(static=True),
				'my_task':MyTaskB()
			}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, 'out_A.txt')))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))


class TestStaticRunner(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)
		if os.path.exists('./linguini_markers'):
			shutil.rmtree('./linguini_markers')

	def test_static_runner(self):
		'''
			Tests that, when a runner is invoked with the keyword argument
			static=True, it will not inherit it's parent's lot name, and it
			will always run with pilot=False, even if it's parent is run
			with pilot=True.
		'''

		LOT_NAME = 'my_lot'
		LOT_NAME_A = 'my_lot_A'
		LOT_NAME_B = 'my_lot_B'

		# make a files to be read, one prepends pilot, the other doesn't
		# and one uses the lot name of the subrunner, the other uses that
		# of the parent runner
		open(os.path.join(
			TEST_DIR, '%s_in_A.txt' % LOT_NAME_A), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTaskA(SimpleTask):
			outputs = File(TEST_DIR, 'out_A.txt')
			inputs = File(TEST_DIR, 'in_A.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')

		class MyTaskB(SimpleTask):
			outputs = File(TEST_DIR, 'out_B.txt')
			inputs = File(TEST_DIR, 'in_B.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')

		class SubRunnerA(Runner):
			lot = LOT_NAME_A
			tasks = {'my_task':MyTaskA()}

		class SubRunnerB(Runner):
			lot = LOT_NAME_B
			tasks = {'my_task':MyTaskB()}

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'runner_A': SubRunnerA(static=True),
				'runner_B': SubRunnerB()
			}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_out_A.txt'%LOT_NAME_A)))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))


	def test_ignore_pilot_runner(self):
		'''
			Tests that, when a runner is instantiated with the keyword 
			argument ignore_pilot=True, it will be run with pilot=False even
			if its parent is run with pilot=True.
		'''

		LOT_NAME = 'my_lot'
		LOT_NAME_A = 'my_lot_A'
		LOT_NAME_B = 'my_lot_B'

		# make a files to be read, one prepends pilot, the other doesn't
		open(os.path.join(
			TEST_DIR, '%s_in_A.txt' % LOT_NAME), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTaskA(SimpleTask):
			outputs = File(TEST_DIR, 'out_A.txt')
			inputs = File(TEST_DIR, 'in_A.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')

		class MyTaskB(SimpleTask):
			outputs = File(TEST_DIR, 'out_B.txt')
			inputs = File(TEST_DIR, 'in_B.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')

		class SubRunnerA(Runner):
			lot = LOT_NAME_A
			tasks = {'my_task':MyTaskA()}

		class SubRunnerB(Runner):
			lot = LOT_NAME_B
			tasks = {'my_task':MyTaskB()}

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'runner_A': SubRunnerA(ignore_pilot=True),
				'runner_B': SubRunnerB()
			}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		# but both sub-runners still inherit their parent's lot name 
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_out_A.txt'%LOT_NAME)))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))



	def test_share_runner(self):
		'''
			Tests that, when a runner is instantiated with the keyword 
			argument ignore_pilot=True, it will be run with pilot=False even
			if its parent is run with pilot=True.
		'''

		LOT_NAME = 'my_lot'
		LOT_NAME_A = 'my_lot_A'
		LOT_NAME_B = 'my_lot_B'

		# make files to be read, one uses lot name of the subrunner, the other
		# uses lot name of the parent runner.  Both prepend with pilot.
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_A.txt' % LOT_NAME_A), 'w').write('oy')
		open(os.path.join(
			TEST_DIR, '%s_pilot_in_B.txt'%LOT_NAME), 'w').write('oy')

		class MyTaskA(SimpleTask):
			outputs = File(TEST_DIR, 'out_A.txt')
			inputs = File(TEST_DIR, 'in_A.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')

		class MyTaskB(SimpleTask):
			outputs = File(TEST_DIR, 'out_B.txt')
			inputs = File(TEST_DIR, 'in_B.txt')

			def run(self):
				self.inputs.open('r').read()
				self.outputs.open('w').write('yo')

		class SubRunnerA(Runner):
			lot = LOT_NAME_A
			tasks = {'my_task':MyTaskA()}

		class SubRunnerB(Runner):
			lot = LOT_NAME_B
			tasks = {'my_task':MyTaskB()}

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'runner_A': SubRunnerA(share=True),
				'runner_B': SubRunnerB()
			}

		# this will cause an error if the input prepended the input file name
		MyRunner().run(pilot=True)

		# check that the output was also not prepended by pilot
		# but both sub-runners still inherit their parent's lot name 
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_A.txt'%LOT_NAME_A)))
		self.assertTrue(os.path.isfile(
			os.path.join(TEST_DIR, '%s_pilot_out_B.txt'%LOT_NAME)))


class TestClobberInheritance(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)

	def tearDown(self):
		shutil.rmtree(TEST_DIR)
		if os.path.exists('./linguini_markers'):
			shutil.rmtree('./linguini_markers')

	
	def test_resource_inheritance(self):

		# a Resource's clobber flag is set by get_ready
		my_file = File('.', 'file.txt')
		my_file.get_ready(lot='my_lot', pilot=False, name='name', 
			clobber=True)
		self.assertTrue(my_file.get_clobber())

		# it's overriden by instantiation keyword argument
		self.assertTrue(File('.', 'file.txt', clobber=True).get_clobber())

		# instantiation gets the last word
		my_file = File('.', 'file.txt', clobber=True)
		my_file.get_ready(lot='my_lot', pilot=False, name='name', 
			clobber=False)
		self.assertTrue(my_file.get_clobber())

		# instantiation gets the last word
		my_file = File('.', 'file.txt', clobber=False)
		my_file.get_ready(lot='my_lot', pilot=False, name='name', 
			clobber=True)
		self.assertFalse(my_file.get_clobber())


		class MyFile(File):
			clobber = True

		# this resource clobbers by default
		self.assertTrue(MyFile('.', 'file.txt').get_clobber())

		# that behavior is overriden by instantiation keyword argument
		self.assertFalse(MyFile('.', 'file.txt', clobber=False).get_clobber())

		# it's not overridden by argument passed to get_ready
		my_file = MyFile('.', 'file.txt')
		my_file.get_ready(lot='my_lot', pilot=False, name='name', 
			clobber=False)
		self.assertTrue(my_file.get_clobber())


		class MyFile(File):
			clobber = False

		# this resource does not clobber by default
		self.assertFalse(MyFile('.', 'file.txt').get_clobber())

		# that behavior is overriden by instantiation keyword argument
		self.assertTrue(MyFile('.', 'file.txt', clobber=True).get_clobber())

		# it's not overridden by argument passed to get_ready
		my_file = MyFile('.', 'file.txt')
		my_file.get_ready(lot='my_lot', pilot=False, name='name', 
			clobber=True)
		self.assertFalse(my_file.get_clobber())



	def test_task_inheritance(self):

		LOT_NAME = 'my_lot'

		open(os.path.join(TEST_DIR, '%s_a.txt'%LOT_NAME), 'w').write('no')
		open(os.path.join(TEST_DIR, '%s_b.txt'%LOT_NAME), 'w').write('no')

		class MyTask(SimpleTask):
			outputs = {
				'A': File(TEST_DIR, 'a.txt'),
				'B': File(TEST_DIR, 'b.txt')
			}
			def run(self):
				self.outputs['A'].open('w').write('yes')
				self.outputs['B'].open('w').write('yes')

		task = MyTask()

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'task1' : task
			}


		# by default task has clobber = False 
		with self.assertRaises(IOError):
			MyRunner().run()

		self.assertFalse(task.get_clobber())
		self.assertEqual(
			'no', 
			open(os.path.join(TEST_DIR, '%s_a.txt'%LOT_NAME), 'r').read()
		)
		self.assertEqual(
			'no', 
			open(os.path.join(TEST_DIR, '%s_b.txt'%LOT_NAME), 'r').read()
		)

		# contents should not have changed, but reset for certainty
		open(os.path.join(TEST_DIR, '%s_a.txt'%LOT_NAME), 'w').write('no')
		open(os.path.join(TEST_DIR, '%s_b.txt'%LOT_NAME), 'w').write('no')

		# this gets overidden by keyword argument to runner
		MyRunner().run(clobber=True)

		self.assertTrue(task.get_clobber())
		self.assertEqual(
			'yes', 
			open(os.path.join(TEST_DIR, '%s_a.txt'%LOT_NAME), 'r').read()
		)
		self.assertEqual(
			'yes', 
			open(os.path.join(TEST_DIR, '%s_b.txt'%LOT_NAME), 'r').read()
		)


		task = MyTask(clobber=True)

		class MyRunner(Runner):
			lot = LOT_NAME
			tasks = {
				'task1' : task
			}



class TestClobberMode(TestCase):

	def setUp(self):
		os.mkdir(TEST_DIR)


	def tearDown(self):
		shutil.rmtree(TEST_DIR)
		if os.path.exists('./linguini_markers'):
			shutil.rmtree('./linguini_markers')


	def test_folder_resource(self):
		'''
			test that when a task is run in clobber mode, the Folder
			resource is willing to overwrite files
		'''
		subfolder = os.path.join(TEST_DIR, 'subfolder')
		os.mkdir(subfolder)

		class MyTask(SimpleTask):
			outputs = Folder(TEST_DIR, 'subfolder')
			def run(self):
				self.outputs.open('yo.txt', 'w').write('yo')

		class MyRunner(Runner):
			lot='my_lot'
			tasks = {
				'task': MyTask()
			}

		MyRunner().run(clobber=True)
		MyRunner().run(clobber=True)

	
	def test_simple_task_inherit(self):

		runs = []
		class MyTask(SimpleTask):
			def run(self):
				runs.append('yo')

		class MyRunner(Runner):
			lot = 'my_runner'
			tasks = {
				'task': MyTask()
			}

		# to begin with we haven't run the runner yet
		self.assertEqual(len(runs), 0)

		# after first call to run, we can see it has run once
		MyRunner().run()
		self.assertEqual(len(runs), 1)

		# second call to run does not cause it to run 
		MyRunner().run()
		self.assertEqual(len(runs), 1)

		# we can force it to run again using clobber
		MyRunner().run(clobber=True)
		self.assertEqual(len(runs), 2)

		# now we test the clobber argument when added to the Tasks class
		# definition
		

	def test_simple_task_invocation(self):

		runs = []

		class MyTask(SimpleTask):
			clobber = True
			def run(self):
				runs.append('yo')

		my_task = MyTask()

		class MyRunner(Runner):
			lot = 'my_runner'
			tasks = {
				'task': my_task
			}


		# to begin with we haven't run the runner yet
		self.assertEqual(len(runs), 0)

		# after first call to run, we can see it has run once
		MyRunner().run()
		self.assertEqual(len(runs), 1)

		self.assertTrue(my_task.get_clobber())

		# second call will run it, because the class def has clobber=True
		MyRunner().run()
		self.assertEqual(len(runs), 2)

		## we can force it to run again using clobber
		#MyRunner().run(clobber=True)
		#self.assertEqual(len(runs), 2)



		





if __name__ == '__main__':
	unittest.main()

