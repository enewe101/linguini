.. linguini documentation master file, created by
   sphinx-quickstart on Thu Apr  9 18:58:48 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to linguini's documentation!
====================================

Linguini provides tools for rapidly prototyping data analysis pipelines in a
self-documenting way.  You'll like linguini if you find yourself saying the
following things:
 - Which directory had the second run's data again?
 - Did we deduplicate that data already?
 - Did we run this using the new analysis module, or the old one?
 - We reran the entire pipeline a thousand times with tiny variations and
   I can't tell what's what!!
 - What code generated this data!?
 - What were the processing steps, from start to finish?


The data pipeline problem
=========================
Linguini seeks to achieve the following goals:
 - specify the architecture of entire data processing pipelines
 - collect all parameters that define the pipeline, such as file paths,
   commandline options, and so on, into the specification
 - execute the data processing based on the specification
 - resume processing after an interruption, without executing 
   parts of the pipeline that completed successfully
 - run variations of a pipelines, while transparently keeping data from 
   different versions separate from one another


Concepts
========
Linguini is designed to help you build data pipelines.  A pipeline is 
represented by the ``Runner`` class, which is also responsible for managing 
the execution of the pipeline.  The pipeline is built up from ``Resource``\s
and ``Task``\s.  Resources represent things like files, or a file system,
a network resource, a database, basically, anything that yields or accepts
data to or from intermediate processing steps.  Tasks do the actual 
computations.


Tasks
~~~~~
The Task is the easiest concept to understand, because its role is clear:
a task carries out some unit of arbitrary processing, like parsing a bunch
of files, or training a machine learning algorithm.

Linguini provides the Task class, which is used to wrap around some processing 
while providing a consistent interface so that Tasks can be assempled together
into a pipeline.  To specify a task, you subclass the Task class, and define
three things: the class-level variables ``inputs`` and  ``outputs``, and a 
``run`` method.

The ``run`` method should carry out all of the proccessing you want the task
to accomplish.  The ``inputs`` and ``outputs`` are used to determine whether
the task is ready to run, or whether it was already run previously.  We'll 
see how that works in the next section.  For now, let's have a look at a simple
task::

    from linguini import Task, FileResource

    PATH = '.'

    class MyTask(Task):
        outputs = FileResource(PATH, 'out.txt')
        inputs = FileResource(PATH, 'in.txt')
        
        def run(self):
            in_fh = self.inputs.open('r')
            out_fh = self.outputs.open('w')
            out_fh.write('\n'.join(reversed(in_fh.readlines())))

This task writes the lines from out.txt, in reverse order, to out.txt.

One facility that linguini tasks have is that they can tell whether they
have already been executed.  Generally, a task considers itself to have 
already executed, if all of its outputs already exist.  In the simple case
above, where the output is the file out.txt, the task knows that it only needs
to run if out.txt doesn't exist yet.


Resources
~~~~~~~~~
The Resource class provided by linguini is used to represent sources or stores
of data.  Wrapping a data store in the linguini resource class provides three
things, the first of which is helping to determine whether a task needs to run.
Usually, a task considers itself to have run if all of its output resources
exist.  It determines that by calling each of its output's ``exist`` method.
For ``FileResource``\s, this is determined by looking for the file in the 
file system.  But a resource can check for any arbitrary condition to decide
whether it exits.

Resources also help to keep data from different processing runs separate
from one another, without needing to fuss around with prefixing file names.
For example, the default behavior of the FileResource is to prefix files
by a ``lot``, which is a class level variable of the Runner, the class
responsible for executing a network of tasks.

Another thing that resources provide is a way to execute pilot-scale runs of
your data pipeline.  Resources have access to the ``pilot`` flag, which is
a class-level variable of the Runner executing the tasks, and if it set to
true, they can choose to only read (or write) a fraction of the data they
control.  Usually, its only necessary to control how much data is *read*.

Generally, resources are conservative: they don't overwrite data unless 
explicitly told to.


Runners
~~~~~~~
The Runner class is used to configure and run data processing networks.
A new Runner should be made by subclassing the Runner class, and specifying
three things: the class-level variables ``lot``, ``tasks``, and ``layout``.

Here is an example of a simple Runner::

    from linguini import Runner

    class MyRunner(Runner):
        lot = 'my_first_lot'
        tasks = {
            'task1': MyTask()
        }
        layout = {
            'END': 'task1'
        }

This runner could then be executed by instantiating it and calling its
run method: ``MyRunner().run()``.

The ``lot`` is used to establish a namespace used by all of the resources 
that get used by tasks in the runner.  This is what makes it possible to
transparently separate data generated by two different, but similar, Runners.

The ``tasks`` and ``layout`` variables respectively identify the tasks that
the runner will execute, and how tasks depend on eachother.  This will be 
easier to understand by looking at a slightly more complicated runner::

    class MyRunner (Runner):
        lot = 'my_second_lot'
        tasks = {
            'task1': MyTaskA(),
            'task2': MyTaskB(),
            'task3': MyTaskC(),
            'task4': MytaskD()
        }
        layout = {
            'END': 'task4',
            'task4': ['task3', 'task2'],
            'task2': 'task1'
        }

The keys of the layout dict correspond to tasks, and the values correspond to
dependencies.  The special key 'END' is used to specify what must be completed
for the entire Runner to be considered complete.  The values in the layout
can either be single tasks (identified by the names givent to them in the 
tasks dict), or lists of tasks.  This is also true for the 'END' item.  In
order for all of the tasks in a network to be executed, all of the "sinks"
(that is, tasks which themselves have no dependencies) need to be listed in 
the 'END' entry.

A full example
==============
Suppose that we want to do the following:
 - scrape a bunch of news articles from the New York Times and the New Yorker
   (which are two different news magazines), and store the articles locally as
   raw html files
 - parse the html files, extracting the title, magazine name, and article
   text, and storing the normalized result in JSON format
 - use the parsed files as examples to train a machine learning algorithm
   to recognize whether an article comes from the New Yorker or the New York
   Times

We'll assume that we already have these functions / classes defined elsewhere:
 - parse_nyker(): parses an html file representing a New Yorker article,
  yielding a dictionary that has the magazine name, title, and article text
 - parse_nyt(): similar, but performs it on html files representing a New York Times article 
 - TfidfCalculator(): computes tfidf vectors for each word occuring in a 
   given article from a set of articles
 - train_ml(): trains a machine learning algorithm to distinguish New York 
   Times articles from New Yorker articles, based on the tfidf scores
   
In other words, we'll assume that the details of the task implementation are
taken care of, so we can see how these tasks can be assembled using linguini.

First, let's define the task for parsing new yorker files::

    class ParseNykr(Task):
        inputs = FileIterReader(
            in_dir='nyt_html', 
            in_fname=r'.*\.txt',
            out_dir='nyt_json',
            out_fname='%s.txt'
        )

        outputs = FileIterWriter(
            in_dir='nyt_html', 
            in_fname=r'.*\.txt',
            out_dir='nyt_json',
            out_fname='%s.txt'
        )

        def run(self):
            for in_fh, in_fname in self.inputs:
                out_fh = self.outputs.open(in_fname, 'w')
                extracted_data = parse_nykr(in_fh.read())
                out_fh.write(json.dumps(extracted_data))

This uses linguini's the FileIterReader and FileIterWriter resource classes,
which are useful for making one-to-one file transformations.  The 
FileIterReader yields file handles from the director identified by ``in_dir``,
restricting itself to those that match ``fname``, which are not already
found in ``out_dir``.  Meanwhile, the FileIterWriter knows that it it should
have one in its out_dir for every matched file in its in_dir.  Since these
resources represent lots of files, their filenames are specified by a 
regular expression (for matching files in the in_dir) and a formatting_string
(allowing substitution to name files in the out_dir).

The class for parsing New York Times articles would look similar, so we
don't need to see it.

Next, we need to compute tfidf scores over the entire set of parsed files
for both the New York Times and New Yorker json files::

    class ComputeTfidf(Task):
        inputs = {
            'nyt_reader': FileIterReader(
                in_dir='nyt_json', in_fname=r'.*\.json'
            ), 
            'nyt_writer': FileIterReader(
                in_dir='nyker_json', in_fname=r'.*\.json'
            )
        }
        outputs = FileResource('.', 'tfidf.csv')

        def run(self):
            tfidf_calculator = TfidfCalculator()

            for in_fh, in_fname in self.inputs['nyt_reader']:
                article_text = json.loads(in_fh.read())
                tfidf_calculator.add(in_fname, article_text)

            for in_fh, in_fname in self.inputs['nykr_reader']:
                article_text = json.loads(in_fh.read())
                tfidf_calculator.add(in_fname, article_text)

            out_fh = self.outputs.open('w')
            out_fh.write(tfidf_calculator.dump())

Finally, the machine learning step would simply consume the tfidf.csv file
and use it for training::

    class TrainClassifier(Task):
        inputs = FileResource('.', 'tfidf.csv')
        outputs = FileResource('.', 'clasifier.model')

        def run(self):
            model = train_ml(self.inputs.open('r').read())
            outputs.open('w').write(model.serialize())

Naturally, all of the details in parsing files and actually doing the training
of the classifier are hidden behind functions that we have assumed are defined
elsewhere.  And in general, this is a good thing, since it allows you to 
keep the detailed logic of how tasks are actually carried out separate from
the Task abstraction, which is concerned with how tasks depend on one another
and when they should be executed.

These tasks can now be pulled together into this one Runner like so::

    class ArticleTrainer(Runner):
        lot = '1'
        tasks = {
            'parse_nykr': ParseNykr(),
            'parse_nyt': ParseNyt(),
            'compute_tfidf': ComputeTfidf(),
            'train': TrainClassifier()
        }
        layout = {
            'END': 'train',
            'train': 'compute_tfidf',
            'compute_tfidf': ['parse_nykr', 'parse_nyt']
        }


How resources are namespaced
============================
One of the reasons for delegating resource handling to the resource class is
that it it provides a consistent interface for establishing the namespace 
of a given Runner.  All runners *must* define a ``lot``, and this lot is 
automatically available inside the resource as ``self.lot``.

Exactly how a given resource uses the lot name to prevent collisions depends
on the resource.  FileResources append the lot name in front of the given 
file name.  So, when file resource ``FileResource('.', 'test.txt')`` is used 
inside a Runner whose lot is 'my_lot', the actual file written will be 
``./my_lot_test.txt``.  This naming is also used when the fileResource is 
used to read files.


Reusing tasks with parameters
=============================
Often times in a project, a given process may be executed many times, with 
small variations.  It would seem like too much typing to re-define new Tasks
to that simply set flags differently to underlying function calls.  
Furthermore, that tends to make it difficult to see what is different between
two apparently similarly defined tasks.

To handle this case, task can be given parameters when they are instantiated.
The default Task class does not take any positional parameters, and 
stores all of its keyword arguments in a self.parameters dictionary.
These can then be accessed within the run function.


Simpler progress tracking
=========================
It's not always easy to determine whether a Task has already been performed
based on its outputs.  For example, a task that appends to a file, rather
than creates it, can't rely on the existence of its output to determine whether
it was done.  

Of course, the most robust solution is to condition on whether the result that
that task was meant to produce was produced, but that could be expensive and
tricky to determine.

A simple workaround is to use the MarksProgress mixin.  A task which inherits
from the MarksProgress class automatically creates a file at 
``./<lot_name>_<marker_name>.marker``, and populates it with the timestamp
that the task was completed.  Existence of *this* file is used to decide
whether the task should be scheduled by the runner.


Combining Runners
=================
Surprise! Runners are themselves just tasks with some extra functionality.
So, runners can be combined and executed by another runner.  This makes it 
easier to swap in and out major components of a pipeline, such as changing
the pretreatment of data while keeping the analysis all the same, or vice
versa.

When combining runners, namespacing is handled in a special way.  In general
the child runners lot gets changed to <parent_lot>.<child_lot>.  That way,
a two instances of the same child runner still have separate namespaces when
used in different parent runners.

This won't handle all cases though, so a runners lot can be controlled using
the ``lot='some_lot_name'`` keyword argument passed when it is instantiated, 
or by passing ``inherit_lot=True``.  The latter causes the parent's lot name
to be used as the child's rather than prepended to the the child's.


Understanding run property propogation
======================================
One of the basic facilities that linguini provides is the propogation of 
run properties (lot, as_pilot, clobber) down to the tasks and resources
in the run.  This allows you to write code that is aware of these properties, 
without having to repeatedly write logic that synchronizes the properties
every time you create a new task or resource.

Not all properties propogate in the same way.  For example, the lot property
doesn't get propogated to sub-runs, as_pilot and clobber do.  This is because
one of the main reasons for using subruns is to share upstream infrastructure
between two runners, without duplicating the resources.  In other words,
sub-runs do not inherit a lot from their parent, because they define their
own lot, and that means they can be shared between different parent runs
without duplicating resources.

In short, propogation works like this:

 - ``lot``: to ordinary tasks and resources, but not sub-runs
 - ``as_pilot``: propogated to tasks and resources.
 - ``clobber``: propogated to tasks and resources.


Overriding property propogation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sometimes you may want to be able to override the propogation of properties.
For the case of ``as_pilot`` and ``clobber``, this can be done easily in 
two ways.  If class level variables ``as_pilot`` or ``clobber`` exist on
the sub-run, task, or resource, they will be used.  Alternatively, if these
are passed as keyword arguments to the sub-run, task, or resource during 
instantiation of them, then that value will be used.  For example::

    class MyTask(Task):

        as_pilot = False
        outputs = None

        def run():
            do_stuff()

    class MyRunner(Runner):
        lot = 'my_lot'
        tasks = {
            'task1': MyTask()
        }
        layout = {
            'END': 'task1'
        }

    MyRunner.run(as_pilot=True)

In the above case, the runner has been run in pilot mode, but because the 
task explicitly sets ``as_pilot = False`` in its class definition, that 
task will still be run at full scale.

Another way to achieve this is like so::

    class MyTask(Task):

        ouptuts = None

        def run():
            do_stuff()

    class MyRunner(Runner):
        lot = 'my_lot'
        tasks = {
            'task1': MyTask(as_pilot=False)
        }
        layout = {
            'END': 'task1'
        }

    MyRunner.run(as_pilot=True)

In this case, the runner has again be run in pilot mode.  This is not
overriden in the task's class definition, but, within the runner, it is 
overriden in the task's instantiation.  In the end, the same effect is 
achieved: even though the runner is invoked with ``as_pilot=True``, it is
overriden.

If a property like ``as_pilot`` or ``clobber`` is set both in a tasks class
definition, and passed as a keyword argumument during it's instantiation, it
is the instantiation keyword argument that takes precedence.

As far as ordinary tasks (not sub-runs) and resources are concerned, 
propogation of the ``lot`` property works the same way.  However, the best 
practice is that 
``lot`` should never be set on a task or resource, either in its class 
definition or invocation.  The purpose of Runners is to organize tasks into
lots, in a visible way; setting the lot on a task would leads to the hidden
creation of new lots.  If you find yourself wanting to override the 
lot on a certain task or set of tasks, it suggests that it needs to be 
encapsulated in a runner.


Propogation of lot
~~~~~~~~~~~~~~~~~~
In cases where a parent runner has a sub-runner as a task, the ``lot`` 
property will not be inherited by the sub-runner or its tasks and resources.
This allows runners to share sub-runners in such a way that the sub-runners
don't duplicate execution or resources.

There are cases where this behavior isn't the desired one.  Suppose that 
two runners share the same final set of analysis steps, but that they differ
in the early tasks that generate the data to be analyzed.  It would be
nice to package the analysis steps into a sub-runner, so that it could be 
shared.  But obviously, the analysis steps do need to be duplicated for both
parent runners, with resources kept separate.  In this case, the best practice
is to set the lot of the shared runner by keyword argument during its 
instantiation within each parent runner::

    class SubRunner(Runner):
        lot = 'sub'
        tasks = {
            'some_task': SomeTask()
        }
        layout = {
            'END': 'some_task'
        }

    class ParentRunnerA(Runner):
        lot = 'A'
        tasks = {
            'pt': PretretmentA(),
            'analysis': SubRunner(lot='A')
        }
        layout = {
            'END': 'analysis',
            'analysis': 'pt'
        }

    class ParentRunnerB(Runner):
        lot = 'B'
        tasks = {
            'pt': PretretmentB(),
            'analysis': SubRunner(lot='B')
        }
        layout = {
            'END': 'analysis',
            'analysis': 'pt'
        }
        
        
Here, ``ParentRunnerA`` and ``ParentRunnerB`` both have the same final 
analysis, and they use the same subrunner, but the subrunner is instantiated
with different lots.  Here, the sub-run is given the parents lot.  
Another way to look at it is that runners, by default, don't take the 
lot of their parents, because they represent their own namespace, but they
can be forced to inherit their parents lot.

For most cases, the above technique could work.  But you way wish to set
the sub-runners lot dynamically.  For example, here we have manually set the
sub-runner's lot to be that of its parent's, but what if it's parent's lot were
also to be inherited?  For maximum extensibility, you propogate the lot 
by using dynamic setting of the task parameter by defining a ``_tasks()``
method::

    class SubRunner(Runner):
        lot = 'sub'
        tasks = {
            'some_task': SomeTask()
        }
        layout = {
            'END': 'some_task'
        }

    class ParentRunnerA(Runner):
        lot = 'A'
        def _tasks = {
            'pt': PretretmentA(),
            'analysis': SubRunner(lot=self.lot)
        }
        layout = {
            'END': 'analysis',
            'analysis': 'pt'
        }

This is the preferred way to propogate a lot to a sub-runner, because the
sub-runner's lot is guaranteed to be equal to it's parent's without knowing
the parent's lot ahead of time.







Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

