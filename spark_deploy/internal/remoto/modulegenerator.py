import distutils.sysconfig as sysconfig
import itertools
import internal.util.fs as fs
import os
import re
import sys
import types

from internal.util.printer import *
'''
Idea 0: Use lint on a file for imports. Once we need to generate a module, containing input modules x, y, z, we read x, y, z for the lints, and add the content recursively to the file.
Idea 1: Just manually specify the modules to write.
Potential problems:
 1. Naming conflicts -> We don't care about those. The modules we want to add have no naming problems.
 2. Secondary imports (we want to write module A, but A requires B) -> We don't care about those. 
    It is up to the user to also add module B. We need to remove the "import B" statement, however.
    But, then again, do we have any secondary imports?
'''



def _generate_stl_libs():
    '''Generator for stl-names. Does a breadth-first search on the python installation directory. Adds built-in library names.
    Returns:
        `set(str)` containing all known standard-library files.'''
    ret_list = set()
    std_lib = sysconfig.get_python_lib(standard_lib=True)
    std_lib_len = len(std_lib)

    to_visit = list()
    to_visit.append(std_lib)
    found = set()
    while any(to_visit):
        visit_now = to_visit.pop()
        to_visit += list(fs.ls(visit_now, only_dirs=True, full_paths=True))
        found.update(set('.'.join(y[std_lib_len+1:].split(os.path.sep))[:-3] for y in fs.ls(visit_now, only_files=True, full_paths=True) if y[-3:] == '.py' and y[-11:-3] != '__init__'))

    found.update(set(sys.builtin_module_names))
    return found


class ModuleGenerator(object):
    '''Object to quickly construct self-contained modules, for use with remoto.
    Warning: We have several constraints for the input modules/files:
        1. When including a module X which internally imports module Y, Y is provided to the generator too. Failing to do so will produce a faulty module.
        2. When including a module X and a module Y, there are no name conflicts between X and Y.
        3. For all modules X, X does not have statements that import user-provided modules/files using import statements "import A as B", or "from A import a as b".
        4. All uses of user-provided modules/files must be as if the user-provided modules.'''
    def __init__(self):
        self._files = []
        self._stl_modules_cache = []

    def with_module(self, module):
        if not isinstance(module, types.ModuleType):
            raise ValueError('Require a module!')
        if self._is_regular_python(module.__name__):
            raise ValueError('We do not export stl modules. Given stl module: "{}"'.format(module.__name__))
        self._files.append(module.__file__)
        return self

    def with_modules(self, *modules):
        for x in modules:
            self.with_module(x)
        return self

    def with_file(self, filepath):
        if not os.path.isfile(filepath):
            raise ValueError('Path "{}" does not refer to a file.'.format(filepath))
        self._files.append(str(filepath))
        return self

    def with_files(self, *filepaths):
        for x in filepaths:
            self.with_file(x)
        return self

    def _is_regular_python(self, name):
        if not self._stl_modules_cache:
            self._stl_modules_cache = list(_generate_stl_libs())
        return name in self._stl_modules_cache


    def _read_imports(self, silent=False):
        '''Reads imports from all files. Non-stl python libraries are skipped.
        Args:
            silent (optional bool): If set, prints warnings about found non-stl python libraries.

        Returns:
            `(set, set)`: The first set contains all found stl import names using format 'import x', with elements `x`.
                          The second set contains all found stl import names using format 'from x import y (as z)', with elements `(x, y)` and `(x, y, z)`.'''
        regex_import = re.compile(r'^ *import +([a-zA-Z\._0-9]+)', re.MULTILINE)
        regex_from_import = re.compile(r'^ *from +([a-zA-Z\._0-9]+) import +([a-zA-Z\._0-9]+) *(?:as)? *([a-zA-Z\._0-9]+)?', re.MULTILINE)
        found_stl_imports = set()
        found_stl_import_froms = set()

        for x in self._files:
            with open(x, 'r') as f:
                lines = f.read()

                for match in itertools.chain(regex_import.finditer(lines), regex_from_import.finditer(lines)):
                    matchtuple = match.groups()
                    match_importmodule = matchtuple[0]

                    if not self._is_regular_python(match_importmodule):
                        if not silent:
                            printw('(file: {}) Found non-regular import "{}".'.format(x, match_importmodule))
                    else:
                        if len(matchtuple) > 1:
                            found_stl_import_froms.add(matchtuple)
                        else:
                            found_stl_imports.add(matchtuple[0])
        if not any(found_stl_import_froms):
            raise ValueError('Empty from sequence.')
        return found_stl_imports, found_stl_import_froms


    def _read_non_imports(self, filepath):
        '''Reads given `filepath`, strips lines containing Python import statements (careful with comments, don't use multiline statements with ';'), returns as generator.'''
        regex_no_import = re.compile(r'^((?!(?:from .+)? *import.*).)+$', re.MULTILINE)
        with open(filepath, 'r') as f:
            lines = f.read()
            return '\n'.join(x.group(0) for x in regex_no_import.finditer(lines))


    def generate(self, outputpath, silent=False):
        dest_dir = os.path.dirname(outputpath)
        if not os.path.isdir(dest_dir):
            raise ValueError('Output directory "{}" does not exist.'.format(dest_dir))

        stl_imports, stl_imports_from = self._read_imports(silent=silent)
        with open(outputpath, 'w') as f:
            header = '''

################################################################################
# Generated by the meta modulegenerator
# Processed {} files/modules:
{}
################################################################################

'''.format(len(self._files), '\n'.join('#    {}'.format(x) for x in self._files))
            f.write(header)
            importstring = '\n'+'\n'.join('import {}'.format(name) for name in stl_imports)
            importstring += '\n'
            importstring += '\n'.join('from {} import {} as {}'.format(*names) if len(names) == 3 and names[2] != None else 'from {} import {}'.format(*names) for names in stl_imports_from)
            f.write(importstring)
            for x in self._files:
                content = self._read_non_imports(x)
                f.write('''
################################################################################
# Created from file {}
'''.format(x))
                f.write(content)
                f.write('''
################################################################################

''')