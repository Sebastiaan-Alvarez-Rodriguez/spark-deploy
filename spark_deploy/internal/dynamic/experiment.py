import inspect
import sys

from dynamic.metadeploy import MetaDeploy
from experiments.interface import ExperimentInterface
import util.fs as fs
import util.importer as imp
import util.location as loc
import util.ui as ui

from util.printer import *

class Experiment(object):
    '''
    Object to handle communication with user-defined experiment interface
    Almost all attributes are lazy, so the dynamic code is used minimally.
    '''
    def __init__(self, location, clazz):
        self.location = location
        self.instance = clazz()
        self._metadeploy = MetaDeploy()

    @property
    def metaDeploy(self):
        return self._metadeploy

    @property
    def max_nodes_needed(self):
        return self.instance.max_nodes_needed()

    def start(self, index, amount):
        from remote.reserver import reservation_manager
        try:
            self._metadeploy.set_idx_amt(index, amount)
            return self.instance.start(self._metadeploy)
        except Exception:
            print('Registered exception at experimentation runtime, for experiment {}/{}. Cleaning reservations...'.format(index+1, amount))
            reservation_manager.stop_selected(self._metadeploy._reservation_numbers)
            raise

    def stop(self):
        from remote.reserver import reservation_manager
        try:
            return self.instance.stop(self._metadeploy)
        except Exception:
            print('Registered exception at experimentation stoptime, for experiment {}/{}. Cleaning reservations...'.format(index+1, amount))
            raise
        finally:
            reservation_manager.stop_selected(self._metadeploy._reservation_numbers)



# Load experiment. Assumes 'picked' is a string for 1 experiment, relative to <project root>/experiments/
# Returns None on failure, an Experiment on success
def load_experiment(picked):
    item = picked if picked.endswith('.py') else '{}.py'.format(picked)

    if not fs.isfile(loc.get_metaspark_experiments_dir(), item):
        printe('Could not find provided experiment "{}" in experiment location: {}'.format(item, loc.get_metaspark_experiments_dir()))
        return None

    item = fs.join(loc.get_metaspark_experiments_dir(), item)
    try:
        module = imp.import_full_path(item)
        return Experiment(item, module.get_experiment())
    except AttributeError:
        printe('Picked experiment at {} had no get_experiment()!'.format(item))
        return None

# Standalone function to get an experiment instance
def get_experiments():
    candidates = []
    for item in fs.ls(loc.get_metaspark_experiments_dir(), full_paths=True, only_files=True):
        if item.endswith(fs.join(fs.sep(), 'interface.py')) or not item.endswith('.py'):
            continue
        try:
            module = imp.import_full_path(item)
            candidates.append((item, module.get_experiment(),))
        except AttributeError:
            printw('Experiment candidate had no get_experiment(): {}. Skipping for now...'.format(item))
        except SyntaxError as e:
            printw('Experiment candidate "{}" had a syntax error:'.format(item))
            print(e)

    if len(candidates) == 0:
        raise RuntimeError('Could not find a subclass of "ExperimentInterface" in directory {}. Make a ".py" file there, with a class extending "ExperimentInterface". See the example implementation for more details.'.format(loc.get_metaspark_experiments_dir()))
    elif len(candidates) == 1:
        return [Experiment(candidates[0][0], candidates[0][1])]
    else:
        idcs = ui.ask_pick_multiple('Multiple suitable experiments found. Please pick experiments:', [x[0] for x in candidates])
        return [Experiment((candidates[x])[0], (candidates[x])[1]) for idx, x in enumerate(idcs)]