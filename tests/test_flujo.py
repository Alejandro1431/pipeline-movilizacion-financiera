"""Pruebas de la orquestación.

No ejecutan el pipeline: comprueban que el grafo y las políticas de reintento
están declaradas como se decidió. Es justo lo que se rompe en silencio cuando
alguien refactoriza.
"""
import pytest
from prefect import Flow

from pipeline import flujo


class TestDefinicionDelFlujo:
    def test_el_flujo_tiene_un_nombre_estable(self):
        assert isinstance(flujo.pipeline, Flow)
        assert flujo.pipeline.name == 'pipeline-movilizacion-financiera'

    def test_solo_la_tarea_de_red_reintenta(self):
        # La descarga por red es lo único que se arregla reintentando. Reintentar
        # una regla de calidad que falla solo retrasa el aviso de que algo va mal.
        assert flujo.tarea_extraer.retries == 3
        assert flujo.tarea_extraer.retry_delay_seconds == 15

        for tarea in (flujo.tarea_transformar, flujo.tarea_marts,
                      flujo.tarea_calidad, flujo.tarea_perfilar):
            assert tarea.retries == 0, f'{tarea.name} no debería reintentar'

    def test_las_cinco_tareas_estan_definidas(self):
        # El orden real de ejecución no se comprueba aquí: se comprueba en cada
        # ejecución del flujo, en local y en CI. Aquí se fija el conjunto de
        # tareas y sus políticas, que es lo que un refactor puede romper sin que
        # salte nada.
        tareas = (
            flujo.tarea_extraer,
            flujo.tarea_transformar,
            flujo.tarea_marts,
            flujo.tarea_calidad,
            flujo.tarea_perfilar,
        )
        assert {tarea.name for tarea in tareas} == {
            'extraer', 'transformar', 'modelo dimensional', 'calidad', 'perfilar',
        }


class TestPuertasDeCalidad:
    """Las tareas que validan tienen que detener el flujo, no avisar y seguir."""

    def test_la_calidad_falla_si_hay_reglas_incumplidas(self, monkeypatch):
        monkeypatch.setattr(flujo.quality, 'ejecutar', lambda: {
            'total': 2,
            'fallidas': 1,
            'veredicto': 'fail',
            'resultados': [{'regla': 'la clave es única', 'pasa': False}],
        })
        # .fn llama a la función original sin pasar por el motor de Prefect
        with pytest.raises(ValueError, match='la clave es única'):
            flujo.tarea_calidad.fn()

    def test_el_modelo_falla_si_dbt_falla(self, monkeypatch):
        monkeypatch.setattr(flujo.marts, 'ejecutar', lambda: {'run': 0, 'test': 1})
        with pytest.raises(RuntimeError, match='dbt fallo'):
            flujo.tarea_marts.fn()

    def test_el_modelo_pasa_cuando_dbt_va_bien(self, monkeypatch):
        monkeypatch.setattr(flujo.marts, 'ejecutar', lambda: {'run': 0, 'test': 0})
        assert flujo.tarea_marts.fn() == {'run': 0, 'test': 0}
