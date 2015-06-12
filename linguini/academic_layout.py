import os
import sys


if len(sys.argv) == 1:
	project_name = ''
elif len(sys.argv) == 2:
	project_name = sys.argv[1] 
elif len(sys.argv) == 3:
	project_name = sys.argv[2]

settings_base = '''import os

PROJ_DIR = '%s'
DATA_DIR = os.path.join(PROJ_DIR, 'data')''' % os.path.join(
	os.getcwd(), project_name
)

runner_base = '''import sys
sys.path.append('.')
from tasks.my_first_task import MyFirstTask
from tasks.my_second_task import MySecondTask
from linguini import Runner

class MyRunner(Runner):
	lot = 'my_lot'
	tasks = {
		'task1': MyFirstTask(),
		'task2': (MySecondTask(), 'task1')
	}

if __name__=='__main__':
	MyRunner().run()
'''

first_task_base = '''from linguini import Task, SimpleTask, File, Folder
from local_settings import DATA_DIR

class MyFirstTask(SimpleTask):
	inputs = File(DATA_DIR, '0.txt', static=True)
	outputs = File(DATA_DIR, '1.txt')

	def run(self):
		contents = self.inputs.open('r').read()
		self.outputs.open('w').write(contents[::-1]) # reverses contents'''

second_task_base = '''from linguini import Task, SimpleTask, File, Folder
from local_settings import DATA_DIR

class MySecondTask(SimpleTask):
	inputs = File(DATA_DIR, '1.txt')
	outputs = File(DATA_DIR, '2.txt')

	def run(self):
		contents = self.inputs.open('r').read()
		self.outputs.open('w').write(
			'\\n'.join(['\\t'*i + c for i,c in enumerate(contents)])	# stagger
		)'''

input_base = '1234567'

DIRS = {
	'src': ['tasks', 'resources'],
	'docs': ['documentation', 'publications'],
	'data': None
}


FILES = {
	'': ('README.md', ''),
	'src': [
		('local_settings.py', settings_base),
		('run.py', runner_base)
	],
	'src/resources': ('__init__.py', ''),
	'src/tasks': [
		('__init__.py', ''),
		('my_first_task.py', first_task_base),
		('my_second_task.py', second_task_base),
	],
	'data': ('0.txt', input_base)
}

