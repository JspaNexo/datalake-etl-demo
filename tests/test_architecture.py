"""La regla de dependencias se verifica sin instalar servicios ni frameworks."""

import ast
import unittest
from pathlib import Path


class ArchitectureTests(unittest.TestCase):
    def test_domain_and_use_cases_only_depend_on_inner_layers_and_stdlib(self):
        import sys
        root = Path(__file__).resolve().parents[1] / 'src' / 'datalake_demo'
        violations = []
        for layer in ('domain', 'etl'):
            allowed = {'datalake_demo.domain'}
            if layer == 'etl':
                allowed.add('datalake_demo.etl')
            for path in (root / layer).rglob('*.py'):
                for node in ast.walk(ast.parse(path.read_text(encoding='utf-8-sig'))):
                    modules = []
                    if isinstance(node, ast.Import):
                        modules = [alias.name for alias in node.names]
                    elif isinstance(node, ast.ImportFrom):
                        modules = [node.module or '']
                    for module in modules:
                        if module.split('.')[0] in sys.stdlib_module_names:
                            continue
                        if not any(module == prefix or module.startswith(prefix + '.') for prefix in allowed):
                            violations.append(f'{path.name}:{node.lineno}: {module}')
        self.assertEqual(violations, [], '\n'.join(violations))
