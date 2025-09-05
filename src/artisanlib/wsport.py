#
# ABOUT
# WebSocket support for Artisan

# LICENSE
# This program or module is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation, either version 2 of the License, or
# version 3 of the License, or (at your option) any later versison. It is
# provided for educational purposes and is distributed in the hope that
# it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
# the GNU General Public License for more details.

# AUTHOR
# Marko Luther, 2024

import sys
import random
import json
import logging
import asyncio
import websockets
import contextlib
import socket
import re

from contextlib import suppress
from threading import Thread
from typing import Final, Optional, Union, Any, Set, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from artisanlib.main import ApplicationWindow # pylint: disable=unused-import
    from websockets.asyncio.client import ClientConnection # pylint: disable=unused-import

try:
    from PyQt6.QtWidgets import QApplication # @UnusedImport @Reimport  @UnresolvedImport
except ImportError:
    from PyQt5.QtWidgets import QApplication # type: ignore # @UnusedImport @Reimport  @UnresolvedImport

try:
    from PyQt6.QtCore import QTimer, Qt
except ImportError:
    from PyQt5.QtCore import QTimer, Qt  # type: ignore

from artisanlib.util import convertWeight, weight_units
from artisanlib import __version__

_log: Final[logging.Logger] = logging.getLogger(__name__)

class wsport:

    __slots__ = [ 'aw', '_loop', '_thread', '_write_queue', 'default_host', 'host', 'port', 'path', 'machineID', 'lastReadResult', 'channels', 'readings', 'tx',
                    'channel_requests', 'channel_nodes', 'channel_modes', 'connect_timeout', 'request_timeout', 'compression',
                    'reconnect_interval', '_ping_interval', '_ping_timeout', 'id_node', 'machine_node',
                    'command_node', 'data_node', 'pushMessage_node', 'request_data_command', 'charge_message', 'drop_message', 'addEvent_message', 'event_node',
                    'DRY_node', 'FCs_node', 'FCe_node', 'SCs_node', 'SCe_node', 'STARTonCHARGE', 'OFFonDROP', 'open_event', 'pending_events',
                    'ws', 'wst', 'ui_signals_connected' ]

    def __init__(self, aw:'ApplicationWindow') -> None:
        self.aw = aw

        # internals
        self._loop:        Optional[asyncio.AbstractEventLoop] = None # the asyncio loop
        self._thread:      Optional[Thread]                    = None # the thread running the asyncio loop
        self._write_queue: Optional[asyncio.Queue[str]]        = None # the write queue
        self.ui_signals_connected: bool = False

        # connects to "ws://<host>:<port>/<path>"
        self.default_host:Final[str] = '127.0.0.1'
        self.host:str = self.default_host # the TCP host
        self.port:int = 80          # the TCP port
        self.path:str = 'WebSocket' # the ws path
        self.machineID:int = 0
        self.compression:bool = True # activatesd/deactivates 'deflate' compression

        self.lastReadResult:Optional[Dict[str,Any]] = None # this is set by eventaction following some custom button/slider Modbus actions with "read" command

        self.channels:Final[int] = 10 # maximal number of WebSocket channels

        # WebSocket data
        self.tx:float = 0 # timestamp as epoch of last read
        self.readings:List[float] = [-1.0]*self.channels

        self.channel_requests:List[str] = ['']*self.channels
        self.channel_nodes:List[str] = ['']*self.channels
        self.channel_modes:List[int] = [0]*self.channels # temp mode is an int here, 0:__,1:C,2:F

        # configurable via the UI:
        self.connect_timeout:float = 4      # in seconds (websockets default is 10)
        self.request_timeout:float = 0.5    # in seconds
        self.reconnect_interval:float = 0.2 # in seconds # not used for now (reconnect delay)
        # not configurable via the UI:
        self._ping_interval:Optional[float] = 20     # in seconds; None disables keepalive (default is 20)
        self._ping_timeout:Optional[float] = 20      # in seconds; None disables timeouts (default is 20)

        # JSON nodes
        self.id_node:str = 'id'
        self.machine_node:str = 'roasterID'
        self.command_node:str = 'command'
        self.data_node:str = 'data'
        self.pushMessage_node:str = 'pushMessage'

        # commands
        self.request_data_command:str = 'getData'

        # push messages
        self.charge_message:str = 'startRoasting'
        self.drop_message:str = 'endRoasting'
        self.addEvent_message:str = 'addEvent'

        self.event_node:str = 'event'
        self.DRY_node:str = 'colorChangeEvent'
        self.FCs_node:str = 'firstCrackBeginningEvent'
        self.FCe_node:str = 'firstCrackEndEvent'
        self.SCs_node:str = 'secondCrackBeginningEvent'
        self.SCe_node:str = 'secondCrackEndEvent'

        # flags
        self.STARTonCHARGE:bool = False
        self.OFFonDROP:bool = False

        self.open_event:Optional[asyncio.Event] = None # an event set on connecting
        self.pending_events:Dict[int, Union[asyncio.Event, Dict[str,Any]]] = {} # message ids associated with pending asyncio.Event object or result


    def _weight_unit_index(self, unit: Optional[str]) -> Optional[int]:
        if unit is None:
            return None
        u = str(unit).strip().lower()
        try:
            return [w.lower() for w in weight_units].index(u)
        except Exception:
            return None

    def _ensure_ui_signal_connections(self) -> None:
        try:
            if not getattr(self, 'ui_signals_connected', False):
                try:
                    # Prefer explicit queued connection
                    self.aw.setGreenWeightSignal.connect(self.aw._onSetGreenWeight, type=Qt.ConnectionType.QueuedConnection)  # type: ignore
                    self.aw.setRoastedWeightSignal.connect(self.aw._onSetRoastedWeight, type=Qt.ConnectionType.QueuedConnection)  # type: ignore
                    self.aw.setRoastBeansSignal.connect(self.aw._onSetRoastBeans, type=Qt.ConnectionType.QueuedConnection)  # type: ignore
                    self.aw.setRoastBatchSignal.connect(self.aw._onSetRoastBatch, type=Qt.ConnectionType.QueuedConnection)  # type: ignore
                except Exception:
                    # Fallback without explicit type
                    self.aw.setGreenWeightSignal.connect(self.aw._onSetGreenWeight)
                    self.aw.setRoastedWeightSignal.connect(self.aw._onSetRoastedWeight)
                    self.aw.setRoastBeansSignal.connect(self.aw._onSetRoastBeans)
                    self.aw.setRoastBatchSignal.connect(self.aw._onSetRoastBatch)
                self.ui_signals_connected = True
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport UI update signals connected')
        except Exception as e:
            if self.aw.seriallogflag:
                self.aw.addserial(f'wsport UI signal connect exception: {e!r}')

    # request event handling

    async def registerRequest(self, message_id:int) -> asyncio.Event:
        e = asyncio.Event()
        self.pending_events[message_id] = e
        return e

    def removeRequestResponse(self, message_id:int) -> None:
        del self.pending_events[message_id]

    # replace the request event by its result
    async def setRequestResponse(self, message_id:int, v:Dict[str, Any]) -> None:
        if message_id in self.pending_events:
            pe = self.pending_events[message_id]
            if isinstance(pe, asyncio.Event):
                pe.set() # unblock
                self.removeRequestResponse(message_id)
                self.pending_events[message_id] = v

    # returns the response received for request with id or None
    def getRequestResponse(self, message_id:int) -> Optional[Dict[str,Any]]:
        if message_id in self.pending_events:
            v = self.pending_events[message_id]
            del self.pending_events[message_id]
            if not isinstance(v, asyncio.Event):
                return v
        return None


    async def producer(self) -> Optional[str]:
        if self._write_queue is None:
            return None
        return await self._write_queue.get()

    async def consumer(self, message:str) -> None:
        j = json.loads(message)
        if self.aw.seriallogflag:
            self.aw.addserial(f'wsport onMessage(): {j}')
        if self.id_node in j:
            await self.setRequestResponse(j[self.id_node],j)
        elif self.pushMessage_node != ''  and self.pushMessage_node in j:
            pushMessage = j[self.pushMessage_node]
            if self.aw.seriallogflag:
                self.aw.addserial(f'wsport pushMessage {pushMessage} received')
            # extra debug about payload shape
            if self.aw.seriallogflag:
                try:
                    dk = (list(j[self.data_node].keys()) if (self.data_node in j and isinstance(j[self.data_node], dict)) else [])
                    self.aw.addserial(f'wsport pushMessage payload: keys={list(j.keys())}, data_keys={dk}')
                except Exception:
                    pass
            if self.charge_message != '' and pushMessage == self.charge_message:
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport CHARGE message received')
                delay = 0 # in ms
                if self.STARTonCHARGE and not self.aw.qmc.flagstart:
                    # turn recording on
                    self.aw.qmc.toggleRecorderSignal.emit()
                    if self.aw.seriallogflag:
                        self.aw.addserial('wsport toggleRecorder signal sent')
                if self.aw.qmc.timeindex[0] == -1:
                    if self.aw.qmc.flagstart:
                        # markCHARGE without delay
                        delay = 1
                    else:
                        # markCharge with a delay waiting for the recorder to be started up
                        delay = self.aw.qmc.delay * 2 # we delay the markCharge action by 2 sampling periods
                    self.aw.qmc.markChargeDelaySignal.emit(delay)
                    if self.aw.seriallogflag:
                        self.aw.addserial(f'wsport markCHARGE() with delay={delay} signal sent')
            elif self.drop_message != '' and pushMessage == self.drop_message:
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport message: DROP')
                if self.aw.qmc.flagstart and self.aw.qmc.timeindex[6] == 0:
                    # markDROP
                    self.aw.qmc.markDropSignal.emit(False)
                    if self.aw.seriallogflag:
                        self.aw.addserial('wsport markDROP signal sent')
                if self.OFFonDROP and self.aw.qmc.flagstart:
                    # turn Recorder off after two sampling periods
#                    delay = (self.aw.qmc.delay * 2)/1000 # we delay the turning OFF action by 2 sampling periods
#                    await asyncio.sleep(delay)
                    self.aw.qmc.toggleMonitorSignal.emit()
                    if self.aw.seriallogflag:
                        self.aw.addserial('wsport toggleMonitor signal sent')
            elif self.addEvent_message != '' and pushMessage == self.addEvent_message:
                if self.aw.qmc.flagstart and self.data_node in j:
                    data = j[self.data_node]
                    if self.event_node in data:
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport message: addEvent({data[self.event_node]}) received')
                        if self.aw.qmc.timeindex[1] == 0 and data[self.event_node] == self.DRY_node:
                            # addEvent(DRY) received
                            if self.aw.seriallogflag:
                                self.aw.addserial('wsport message: addEvent(DRY) processed')
                            self.aw.qmc.markDRYSignal.emit(False)
                        elif self.aw.qmc.timeindex[2] == 0 and data[self.event_node] == self.FCs_node:
                            # addEvent(FCs) received
                            if self.aw.seriallogflag:
                                self.aw.addserial('wsport message: addEvent(FCs) processed')
                            self.aw.qmc.markFCsSignal.emit(False)
                        elif self.aw.qmc.timeindex[3] == 0 and data[self.event_node] == self.FCe_node:
                            # addEvent(FCe) received
                            if self.aw.seriallogflag:
                                self.aw.addserial('wsport message: addEvent(FCe) processed')
                            self.aw.qmc.markFCeSignal.emit(False)
                        elif self.aw.qmc.timeindex[4] == 0 and data[self.event_node] == self.SCs_node:
                            # addEvent(SCs) received
                            if self.aw.seriallogflag:
                                self.aw.addserial('wsport message: addEvent(SCs) processed')
                            self.aw.qmc.markSCsSignal.emit(False)
                        elif self.aw.qmc.timeindex[5] == 0 and data[self.event_node] == self.SCe_node:
                            # addEvent(SCe) received
                            if self.aw.seriallogflag:
                                self.aw.addserial('wsport message: addEvent(SCe) processed')
                            self.aw.qmc.markSCeSignal.emit(False)
                    elif self.aw.seriallogflag:
                        self.aw.addserial(f'wsport message: addEvent({data})')
                elif self.aw.seriallogflag:
                    self.aw.addserial('wsport message: addEvent() received and ignored. Not recording.')

            # set burner: { "pushMessage": "setBurnerCapacity", "data": { "burnercapacity": 51 } }
            # name of current roast set: {"pushMessage": "setRoastingProcessName", "data": { "name": "Test roast 123" }}
            # note of current roast set: {"pushMessage": "setRoastingProcessNote", "data": { "note": "A test comment" }}
            # fill weight of current roast set: {"pushMessage": "setRoastingProcessFillWeight", "data": { "fillWeight": 12 }}
            elif pushMessage == 'resetRoast':
                data = j[self.data_node] if self.data_node in j and isinstance(j[self.data_node], dict) else {}
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport resetRoast scheduling OFF->ON')
                async def _off_on_sequence() -> None:
                    try:
                        sound_on = bool(data.get('soundOn', True))
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport resetRoast sequence start')
                        # OFF
                        try:
                            if self.aw.qmc.flagon:
                                if sound_on:
                                    try:
                                        self.aw.soundpopSignal.emit()
                                    except Exception:
                                        pass
                                self.aw.qmc.toggleMonitorSignal.emit()  # queued to UI thread
                                if self.aw.seriallogflag:
                                    self.aw.addserial('wsport resetRoast OFF (toggle) emitted')
                            else:
                                if self.aw.seriallogflag:
                                    self.aw.addserial('wsport resetRoast already OFF')
                        except Exception as e:
                            if self.aw.seriallogflag:
                                self.aw.addserial(f'wsport resetRoast OFF exception: {e!r}')
                        # wait for OFF shutdown
                        for _ in range(120):  # up to ~6s
                            if not self.aw.qmc.flagsamplingthreadrunning and not self.aw.qmc.flagon:
                                break
                            await asyncio.sleep(0.05)
                        # ON
                        try:
                            self.aw.qmc.onMonitorSignal.emit()  # queued to UI thread
                            if self.aw.seriallogflag:
                                self.aw.addserial('wsport resetRoast ON emitted')
                        except Exception as e:
                            if self.aw.seriallogflag:
                                self.aw.addserial(f'wsport resetRoast ON exception: {e!r}')
                    except Exception as e:
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport resetRoast sequence exception: {e!r}')
                try:
                    asyncio.create_task(_off_on_sequence())
                except Exception as e:
                    # fallback to UI-thread timer if loop missing
                    if self.aw.seriallogflag:
                        self.aw.addserial(f'wsport resetRoast create_task exception: {e!r}')
                    def _apply_reset() -> None:
                        try:
                            # OFF
                            try:
                                if self.aw.qmc.flagon:
                                    self.aw.qmc.toggleMonitorSignal.emit()
                            except Exception:
                                pass
                            # try ON shortly after; ToggleMonitor will ignore if still shutting down
                            QTimer.singleShot(200, self.aw.qmc.onMonitorSignal.emit)
                        except Exception as e2:
                            if self.aw.seriallogflag:
                                self.aw.addserial(f'wsport resetRoast timer fallback exception: {e2!r}')
                    QTimer.singleShot(0, _apply_reset)
            elif pushMessage == 'setGreenWeight' and self.data_node in j:
                data = j[self.data_node]
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport setGreenWeight scheduling apply')
                def _apply_green() -> None:
                    try:
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setGreenWeight apply callback entered')
                        v_raw = data.get('value')
                        u_raw = data.get('unit')
                        v = float(v_raw)
                        in_unit_idx = self._weight_unit_index(u_raw)
                        current_unit_idx = [w.lower() for w in weight_units].index(self.aw.qmc.weight[2].lower())
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setGreenWeight raw: value={v_raw} unit={u_raw} unitIdx={in_unit_idx}')
                        if in_unit_idx is not None:
                            v = convertWeight(v, in_unit_idx, current_unit_idx)
                        self.aw.qmc.weight = (v, self.aw.qmc.weight[1], self.aw.qmc.weight[2])
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setGreenWeight applied: {v}{self.aw.qmc.weight[2].lower()}')
                        # Update Roast Properties dialog if open
                        dlg = getattr(self.aw, 'editgraphdialog', None)
                        if dlg and dlg is not False and hasattr(dlg, 'updateWeightEdits') and hasattr(dlg, 'weightinedit'):
                            try:
                                dlg.updateWeightEdits(dlg.weightinedit, v)
                            except Exception as e:
                                if self.aw.seriallogflag:
                                    self.aw.addserial(f'wsport setGreenWeight dialog update exception: {e!r}')
                    except Exception as e:
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setGreenWeight exception: {e!r}')
                QTimer.singleShot(0, _apply_green)
                self._ensure_ui_signal_connections()
                # Also emit queued UI-thread signal to ensure execution on GUI thread
                try:
                    v_raw = data.get('value')
                    u_raw = data.get('unit')
                    if v_raw is not None:
                        self.aw.setGreenWeightSignal.emit(float(v_raw), u_raw)
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setGreenWeight emitted UI-thread signal')
                except Exception as e:
                    if self.aw.seriallogflag:
                        self.aw.addserial(f'wsport setGreenWeight emit exception: {e!r}')
            elif pushMessage == 'setRoastedWeight' and self.data_node in j:
                data = j[self.data_node]
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport setRoastedWeight scheduling apply')
                def _apply_roasted() -> None:
                    try:
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setRoastedWeight apply callback entered')
                        v_raw = data.get('value')
                        u_raw = data.get('unit')
                        v = float(v_raw)
                        in_unit_idx = self._weight_unit_index(u_raw)
                        current_unit_idx = [w.lower() for w in weight_units].index(self.aw.qmc.weight[2].lower())
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setRoastedWeight raw: value={v_raw} unit={u_raw} unitIdx={in_unit_idx}')
                        if in_unit_idx is not None:
                            v = convertWeight(v, in_unit_idx, current_unit_idx)
                        self.aw.qmc.weight = (self.aw.qmc.weight[0], v, self.aw.qmc.weight[2])
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setRoastedWeight applied: {v}{self.aw.qmc.weight[2].lower()}')
                        # Update Roast Properties dialog if open
                        dlg = getattr(self.aw, 'editgraphdialog', None)
                        if dlg and dlg is not False and hasattr(dlg, 'updateWeightEdits') and hasattr(dlg, 'weightoutedit'):
                            try:
                                dlg.updateWeightEdits(dlg.weightoutedit, v)
                            except Exception as e:
                                if self.aw.seriallogflag:
                                    self.aw.addserial(f'wsport setRoastedWeight dialog update exception: {e!r}')
                    except Exception as e:
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setRoastedWeight exception: {e!r}')
                QTimer.singleShot(0, _apply_roasted)
                self._ensure_ui_signal_connections()
                # Also emit queued UI-thread signal to ensure execution on GUI thread
                try:
                    v_raw = data.get('value')
                    u_raw = data.get('unit')
                    if v_raw is not None:
                        self.aw.setRoastedWeightSignal.emit(float(v_raw), u_raw)
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setRoastedWeight emitted UI-thread signal')
                except Exception as e:
                    if self.aw.seriallogflag:
                        self.aw.addserial(f'wsport setRoastedWeight emit exception: {e!r}')
            elif pushMessage == 'setRoastBeans' and self.data_node in j:
                data = j[self.data_node]
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport setRoastBeans scheduling apply')
                def _apply_beans() -> None:
                    try:
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setRoastBeans apply callback entered')
                        beans = data.get('beans')
                        if isinstance(beans, str):
                            self.aw.qmc.beans = beans
                            # Update Roast Properties dialog if open
                            dlg = getattr(self.aw, 'editgraphdialog', None)
                            if dlg and dlg is not False and hasattr(dlg, 'beansedit') and hasattr(dlg.beansedit, 'setNewPlainText'):
                                try:
                                    dlg.beansedit.setNewPlainText(beans)
                                except Exception as e:
                                    if self.aw.seriallogflag:
                                        self.aw.addserial(f'wsport setRoastBeans dialog update exception: {e!r}')

                    except Exception as e:
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setRoastBeans exception: {e!r}')
                QTimer.singleShot(0, _apply_beans)
                self._ensure_ui_signal_connections()
                # Also emit queued UI-thread signal
                try:
                    beans = data.get('beans')
                    if isinstance(beans, str):
                        self.aw.setRoastBeansSignal.emit(beans)
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setRoastBeans emitted UI-thread signal')
                except Exception as e:
                    if self.aw.seriallogflag:
                        self.aw.addserial(f'wsport setRoastBeans emit exception: {e!r}')
            elif pushMessage in ('setRoastTitle', 'setRoastingProcessName') and self.data_node in j:
                data = j[self.data_node]
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport setRoastTitle scheduling apply')
                def _apply_title() -> None:
                    try:
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setRoastTitle apply callback entered')
                        t = data.get('title')
                        if not isinstance(t, str):
                            t = data.get('name')
                        if isinstance(t, str):
                            self.aw.qmc.title = t
                            # Update Roast Properties dialog if open
                            dlg = getattr(self.aw, 'editgraphdialog', None)
                            if dlg and dlg is not False and hasattr(dlg, 'titleedit'):
                                try:
                                    if hasattr(dlg.titleedit, 'setEditText'):
                                        dlg.titleedit.setEditText(t)
                                    elif hasattr(dlg.titleedit, 'setCurrentText'):
                                        dlg.titleedit.setCurrentText(t)
                                except Exception:
                                    pass
                            # Update canvas/window title via signal
                            self.aw.setTitleSignal.emit(self.aw.qmc.title, True)
                            if self.aw.seriallogflag:
                                self.aw.addserial(f'wsport setRoastTitle applied: {t!r}')
                    except Exception as e:
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setRoastTitle exception: {e!r}')
                QTimer.singleShot(0, _apply_title)
            elif pushMessage == 'setRoastBatch' and self.data_node in j:
                data = j[self.data_node]
                if self.aw.seriallogflag:
                    self.aw.addserial('wsport setRoastBatch scheduling apply')
                def _apply_batch() -> None:
                    try:
                        if self.aw.seriallogflag:
                            self.aw.addserial('wsport setRoastBatch apply callback entered')
                        bp = data.get('batch_prefix')
                        bn = data.get('batch_number')
                        b_all = data.get('batch')
                        if (bn is None or bp is None) and isinstance(b_all, str):
                            m = re.match(r'^([^0-9]*)([0-9]+)$', b_all.strip())
                            if m:
                                bp = m.group(1)
                                bn = int(m.group(2))
                        if isinstance(bp, str):
                            self.aw.qmc.roastbatchprefix = bp
                        if isinstance(bn, (int, float, str)):
                            try:
                                self.aw.qmc.roastbatchnr = int(bn)
                            except Exception:
                                pass
                        # Update Roast Properties dialog if open
                        dlg = getattr(self.aw, 'editgraphdialog', None)
                        if dlg and dlg is not False:
                            try:
                                roastpos = (f' ({self.aw.qmc.roastbatchpos})' if self.aw.qmc.roastbatchnr != 0 else '')
                                batch_txt = ('' if self.aw.qmc.roastbatchnr == 0 else f"{self.aw.qmc.roastbatchprefix}{self.aw.qmc.roastbatchnr}{roastpos}")
                                if hasattr(dlg, 'batchedit') and hasattr(dlg.batchedit, 'setText'):
                                    dlg.batchedit.setText(batch_txt)
                            except Exception as e:
                                if self.aw.seriallogflag:
                                    self.aw.addserial(f'wsport setRoastBatch dialog update exception: {e!r}')
                    except Exception as e:
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport setRoastBatch exception: {e!r}')
                QTimer.singleShot(0, _apply_batch)
                self._ensure_ui_signal_connections()
                # Also emit queued UI-thread signal
                try:
                    bp = data.get('batch_prefix')
                    bn = data.get('batch_number')
                    b_all = data.get('batch')
                    self.aw.setRoastBatchSignal.emit(bp, bn, b_all)
                    if self.aw.seriallogflag:
                        self.aw.addserial('wsport setRoastBatch emitted UI-thread signal')
                except Exception as e:
                    if self.aw.seriallogflag:
                        self.aw.addserial(f'wsport setRoastBatch emit exception: {e!r}')


    async def split_and_consume_message(self, message:str) -> None:
        # a message may contain several lines of JSON data separated by \n, like in "{'a':1}\n{'b':2}\n"
        for m in message.strip().split('\n'):
            single_message = m.strip()
            if single_message != '':
                await self.consumer(single_message)

    async def consumer_handler(self, websocket:'ClientConnection') -> None:
        async for message in websocket:
            if isinstance(message, str):
                await self.split_and_consume_message(message)
            elif isinstance(message, bytes):
                await self.split_and_consume_message(message.decode('utf-8'))


    async def producer_handler(self, websocket:'ClientConnection') -> None:
        while True:
            message = await self.producer()
            if message is not None:
                await websocket.send(message)
                await asyncio.sleep(0.1)  # yield control to the event loop


    # if serial settings are given, host/port are ignore and communication is handled by the given serial port
    async def connect(self) -> None:
        if self.aw.qmc.device_logging:
            logging.getLogger('websockets').setLevel(logging.DEBUG)
        else:
            logging.getLogger('websockets').setLevel(logging.ERROR)

        while True:
            try:
                if self.port == 80:
                    hostport = self.host
                else:
                    hostport = f'{self.host}:{self.port}'
                async for websocket in websockets.connect(
                        f'ws://{hostport}/{self.path}',
                        open_timeout = self.connect_timeout,
                        ping_interval = self._ping_interval,
                        ping_timeout = self._ping_timeout,
                        compression = ('deflate' if self.compression else None),
                        origin = websockets.Origin(f'http://{socket.gethostname()}'),
                        user_agent_header = f'Artisan/{__version__} websockets'):
                    done: Set[asyncio.Task[Any]] = set()
                    pending: Set[asyncio.Task[Any]] = set()
                    try:
                        self.aw.sendmessageSignal.emit(QApplication.translate('Message', '{} connected').format('WebSocket'),True,None)
                        if self._write_queue is None:
                            self._write_queue = asyncio.Queue()
                        consumer_task = asyncio.create_task(self.consumer_handler(websocket))
                        producer_task = asyncio.create_task(self.producer_handler(websocket))
                        done, pending = await asyncio.wait(
                            [consumer_task, producer_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        _log.debug('disconnected')
                        for task in pending:
                            task.cancel()
                        for task in done:
                            exception = task.exception()
                            if isinstance(exception, Exception):
                                raise exception
                    except websockets.ConnectionClosed:
                        _log.debug('ConnectionClosed exception')
                        continue
                    except Exception as e: # pylint: disable=broad-except
                        _log.exception(e)
                    finally:
                        for task in pending:
                            task.cancel()
                        for task in done:
                            exception = task.exception()
                            if isinstance(exception, Exception):
                                raise exception
                    _log.debug('reconnecting')
                    self.aw.sendmessageSignal.emit(QApplication.translate('Message', '{} disconnected').format('WebSocket'),True,None)
                    await asyncio.sleep(0.1)
            except asyncio.TimeoutError:
                _log.info('connection timeout')
            except Exception as e: # pylint: disable=broad-except
                _log.error(e)

            self.aw.sendmessageSignal.emit(QApplication.translate('Message', '{} disconnected').format('WebSocket'),True,None)
            await asyncio.sleep(0.2)


    def start_background_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            # run_forever() returns after calling loop.stop()
            loop.run_forever()
            # clean up tasks
            for task in asyncio.all_tasks(loop):
                task.cancel()
            for t in [t for t in asyncio.all_tasks(loop) if not (t.done() or t.cancelled())]:
                with suppress(asyncio.CancelledError):
                    loop.run_until_complete(t)
            self.aw.sendmessageSignal.emit(QApplication.translate('Message', '{} disconnected').format('WebSocket'),True,None)
        except Exception as e:  # pylint: disable=broad-except
            _log.exception(e)
        finally:
            loop.close()

    # start/stop sample thread

    def start(self) -> None:
        try:
            self._loop = asyncio.new_event_loop()
            self._thread = Thread(target=self.start_background_loop, args=(self._loop,), daemon=True)
            self._thread.start()
            # run sample task in async loop
            asyncio.run_coroutine_threadsafe(self.connect(), self._loop)
        except Exception as e:  # pylint: disable=broad-except
            _log.exception(e)

    def stop(self) -> None:
        # self._loop.stop() needs to be called as follows as the event loop class is not thread safe
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop) # pyrefly: ignore
            self._loop = None
        # wait for the thread to finish
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        self._write_queue = None

#    # takes a request as dict to be send as JSON
#    # and returns a dict generated from the JSON response
#    # or None on exception or if block=False
    def send(self, request:Dict[str,Any], block:bool = True) -> Optional[Dict[str,Any]]:
        try:
            if self._loop is None:
                self.start()
            if self._loop is not None:
                message_id = random.randint(1,99999)
                request[self.id_node] = message_id
                if self.machine_node:
                    request[self.machine_node] = self.machineID
                json_req = json.dumps(request, indent=None, separators=(',', ':'), ensure_ascii=True) # we conservatively use escaping for accent characters here despite the utf-8 encoding as some clients might not be able to process non-ascii data
                if self._write_queue is not None:
                    if block:
                        future = asyncio.run_coroutine_threadsafe(self.registerRequest(message_id), self._loop)
                        e = future.result()
                        asyncio.run_coroutine_threadsafe(self._write_queue.put(json_req), self._loop)
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport send() blocking: {json_req}')
                        with contextlib.suppress(asyncio.TimeoutError):
                            asyncio.run_coroutine_threadsafe(asyncio.wait_for(e.wait(), self.request_timeout), self._loop).result()
                        if e.is_set():
                            if self.aw.seriallogflag:
                                self.aw.addserial(f'wsport send() received: {message_id}')
                            return self.getRequestResponse(message_id)
                        if self.aw.seriallogflag:
                            self.aw.addserial(f'wsport send() timeout: {message_id}')
                        self.removeRequestResponse(message_id)
                        return None # timeout
                    asyncio.run_coroutine_threadsafe(self._write_queue.put(json_req), self._loop)
                    if self.aw.seriallogflag:
                        self.aw.addserial(f'wsport send() non-blocking: {json_req}')
                return None
            return None
        except Exception as e: # pylint: disable=broad-except
            _, _, exc_tb = sys.exc_info()
            lineno = 0
            if exc_tb is not None:
                lineno = exc_tb.tb_lineno
            self.aw.qmc.adderror((QApplication.translate('Error Message', 'Exception:') + ' wsport:send() {0}').format(str(e)),lineno)
            return None

    def disconnect(self) -> None:
        self.stop()
