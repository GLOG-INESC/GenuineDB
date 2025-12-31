from dataclasses import dataclass
from common import ColorSchemes

class ExperimentInfo:

    def __init__(self, colors, modules, collapsing_columns, epoch_delay=0):
        self.config = None
        self.label = None
        self.epoch_delay = epoch_delay
        self.colors = colors
        self.modules = modules
        self.collapsing_columns = collapsing_columns

    def initialize(self, config, label):
        self.config = config
        self.label = label

detock_modules = {
    "Server": ["ENTER_SERVER", "EXIT_SERVER_TO_FORWARDER"],
    "Forwarder": ["ENTER_FORWARDER", "EXIT_FORWARDER_TO_SEQUENCER"],
    "Forwarder_Sequencer": ["EXIT_FORWARDER_TO_SEQUENCER", "ENTER_SEQUENCER"],
    "Sequencer": ["ENTER_SEQUENCER", "ENTER_LOCAL_BATCH"],
    "Batcher": ["ENTER_LOCAL_BATCH", "EXIT_SEQUENCER_IN_BATCH"],
    "Sequencer_RLOG": ["EXIT_SEQUENCER_IN_BATCH", "ENTER_LOG_MANAGER_IN_BATCH"],
    "Paxos": ["ENTER_LOG_MANAGER_IN_BATCH", ["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"]],
    "RLOG": [["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"], "EXIT_LOG_MANAGER"],
    "RLOG_Scheduler": ["EXIT_LOG_MANAGER", "ENTER_SCHEDULER"],
    "Scheduler": ["ENTER_SCHEDULER", "ENTER_LOCK_MANAGER"],
    "Lock_Manager": ["ENTER_LOCK_MANAGER", "DISPATCH"],
    "Scheduler_Worker": ["DISPATCH", "ENTER_WORKER"],
    "Worker_total": ["ENTER_WORKER", "EXIT_WORKER"],
    "Worker_Broadcast": ["ENTER_WORKER", "BROADCAST_READ"],
    "Worker_Remote_read": ["BROADCAST_READ", "GOT_REMOTE_READS"],
    "Worker_Execute": ["GOT_REMOTE_READS", "EXECUTE_TXN"],
    "Worker_Server": ["EXIT_WORKER", "RETURN_TO_SERVER"],
    "Server_end": ["RETURN_TO_SERVER", "EXIT_SERVER_TO_CLIENT"],
}

slog_modules = {
    "Server": ["ENTER_SERVER", "EXIT_SERVER_TO_FORWARDER"],
    "Server_Forwarder" : ["EXIT_SERVER_TO_FORWARDER", "ENTER_FORWARDER"],
    "Forwarder": ["ENTER_FORWARDER", "EXIT_FORWARDER_TO_SEQUENCER"],
    "Forwarder_Sequencer": ["EXIT_FORWARDER_TO_SEQUENCER", "ENTER_SEQUENCER"],
    "Forwarder_MH_Order": ["EXIT_FORWARDER_TO_SEQUENCER", "ENTER_MULTI_HOME_ORDERER_IN_BATCH"],
    "MH_Order": ["ENTER_MULTI_HOME_ORDERER_IN_BATCH", "EXIT_MULTI_HOME_ORDERER"],
    "MH_Order_Sequencer": ["EXIT_MULTI_HOME_ORDERER", "ENTER_SEQUENCER"],
    "MH_Order_Batcher": ["EXIT_MULTI_HOME_ORDERER", "ENTER_LOCAL_BATCH"],
    "Batcher": ["ENTER_LOCAL_BATCH", "EXIT_SEQUENCER_IN_BATCH"],

    "Sequencer": ["ENTER_SEQUENCER", "EXIT_SEQUENCER_IN_BATCH"],

    "Sequencer_RLOG": ["EXIT_SEQUENCER_IN_BATCH", "ENTER_LOG_MANAGER_IN_BATCH"],
    "Paxos": ["ENTER_LOG_MANAGER_IN_BATCH", ["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"]],
    "RLOG": [["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"], "EXIT_LOG_MANAGER"],
    "RLOG_Scheduler": ["EXIT_LOG_MANAGER", "ENTER_SCHEDULER"],
    "Scheduler": ["ENTER_SCHEDULER", "ENTER_LOCK_MANAGER"],
    "Lock_Manager": ["ENTER_LOCK_MANAGER", "DISPATCH"],
    "Scheduler_Worker": ["DISPATCH", "ENTER_WORKER"],
    "Worker_total": ["ENTER_WORKER", "EXIT_WORKER"],
    "Worker_Broadcast": ["ENTER_WORKER", "BROADCAST_READ"],
    "Worker_Remote_read": ["BROADCAST_READ", "GOT_REMOTE_READS"],
    "Worker_Execute": ["GOT_REMOTE_READS", "EXECUTE_TXN"],
    "Worker_Server": ["EXIT_WORKER", "RETURN_TO_SERVER"],
    "Server_end": ["RETURN_TO_SERVER", "EXIT_SERVER_TO_CLIENT"],
}

detock_columns_to_collapse = {
    "DISPATCH": ["DISPATCHED_FAST", "DISPATCHED_SLOW", "DISPATCHED_SLOW_DEADLOCKED"]
}

glog_modules = {
    "Enter_Server": ["sent_at", "ENTER_SERVER"],
    "Server": ["ENTER_SERVER", "EXIT_SERVER_TO_FORWARDER"],
    "Forwarder": ["ENTER_FORWARDER", "EXIT_FORWARDER_TO_SEQUENCER"],
    "Forwarder_Sequencer": ["EXIT_FORWARDER_TO_SEQUENCER", "ENTER_SEQUENCER"],
    "Stub": ["ENTER_STUB", "EXIT_STUB"],
    "Stub_Glog": ["EXIT_STUB", "ENTER_GLOG"],
    "GLOG": ["ENTER_GLOG", "EXIT_GLOG"],
    "GLOG_Sequencer": ["EXIT_GLOG", "ENTER_SEQUENCER"],
    "Sequencer": ["ENTER_SEQUENCER", "ENTER_LOCAL_BATCH"],
    "Batcher": ["ENTER_LOCAL_BATCH", "EXIT_SEQUENCER_IN_BATCH"],
    "Sequencer_RLOG": ["EXIT_SEQUENCER_IN_BATCH", "ENTER_LOG_MANAGER_IN_BATCH"],
    "Paxos": ["ENTER_LOG_MANAGER_IN_BATCH", ["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"]],
    "RLOG": [["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"], "EXIT_LOG_MANAGER"],
    "RLOG_Scheduler": ["EXIT_LOG_MANAGER", "ENTER_SCHEDULER"],
    "Scheduler": [["ENTER_SCHEDULER", "ENTER_SCHEDULER_LO"], "ENTER_LOCK_MANAGER"],
    "Lock_Manager": ["ENTER_LOCK_MANAGER", "DISPATCH"],
    "Scheduler_Worker": ["DISPATCH", "ENTER_WORKER"],
    "Worker_total": ["ENTER_WORKER", "EXIT_WORKER"],
    "Worker_Broadcast": ["ENTER_WORKER", "BROADCAST_READ"],
    "Worker_Remote_read": ["BROADCAST_READ", "GOT_REMOTE_READS"],
    "Worker_Execute": ["GOT_REMOTE_READS", "EXECUTE_TXN"],
    "Worker_Server": ["EXIT_WORKER", "RETURN_TO_SERVER"],
    "Server_end": ["RETURN_TO_SERVER", "EXIT_SERVER_TO_CLIENT"],
    "Exit_Server": ["EXIT_SERVER_TO_CLIENT", "received_at"],
}

glog_columns_to_collapse = {
    "DISPATCH": ["DISPATCHED_FAST", "DISPATCHED_SLOW", "DISPATCHED_SLOW_DEADLOCKED"]
}

tiga_modules = {
    "Enter_Server": ["sent_at", "ENTER_SERVER"],
    "Server": ["ENTER_SERVER", "EXIT_SERVER_TO_FORWARDER"],
    "Server_Coord": ["EXIT_SERVER_TO_FORWARDER", "ENTER_COORDINATOR"],
    "Coordinator": ["ENTER_COORDINATOR", "EXIT_COORDINATOR"],
    "Coord_Acceptor": ["EXIT_COORDINATOR", "ENTER_ACCEPTOR"],
    "Acceptor": ["ENTER_ACCEPTOR", "EXIT_ACCEPTOR"],
    "Acceptor_Scheduler": ["EXIT_ACCEPTOR", "ENTER_SCHEDULER"],
    "Scheduler": ["ENTER_SCHEDULER", "DISPATCH"],
    "Scheduler_Worker": ["DISPATCH", "ENTER_WORKER"],
    "Worker": ["ENTER_WORKER", "EXIT_WORKER"],
    "Worker_Server": ["EXIT_WORKER", "RETURN_TO_SERVER"],
    "Server_end": ["RETURN_TO_SERVER", "EXIT_SERVER_TO_CLIENT"],
    "Exit_Server": ["EXIT_SERVER_TO_CLIENT", "received_at"],
}

tiga_columns_to_collapse = {
    "DISPATCH": ["DISPATCH_DEF", "DISPATCH_SPEC"]
}
# Profiling Color schemes
geozip_profiling_colors = ['#f5dd42', '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2',
                           '#7f7f7f', '#bcbd22', '#17becf', '#f7ae62']
detock_profiling_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f',
                           '#bcbd22', '#17becf', '#f5dd42', '#f7ae62']

# Color Schemes


geozip_color_scheme = ColorSchemes(geozip_profiling_colors, "#E69F00")
detock_color_scheme = ColorSchemes(detock_profiling_colors, "#0072B2")
slog_color_scheme   = ColorSchemes(detock_profiling_colors, "#009E73")

detock_fr_color_scheme = ColorSchemes(detock_profiling_colors, "#000099")
glog_fr_color_scheme = ColorSchemes(geozip_profiling_colors, "#ff1100")

glog_color_scheme = ColorSchemes(detock_profiling_colors, "#ff7f0e")
glog_color_scheme_025 = ColorSchemes(detock_profiling_colors, "#990000")
glog_color_scheme_05 = ColorSchemes(detock_profiling_colors, "#000099")
glog_color_scheme_2 = ColorSchemes(detock_profiling_colors, "#009900")

exp_dict = {
    "GLOG" : ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),
    "GeoZip": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),
    "GeoZip_FR": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),

    "GeoZip_0": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),
    "GeoZip_1": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),
    "GeoZip_2": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),

    "GLOG_FURTHEST": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),
    "GLOG_DM": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),
    "GLOG_FR": ExperimentInfo(glog_color_scheme, glog_modules, detock_columns_to_collapse),
    "Detock": ExperimentInfo(detock_color_scheme, detock_modules, detock_columns_to_collapse),

    "DETOCK" : ExperimentInfo(detock_color_scheme, detock_modules, detock_columns_to_collapse),
    "DETOCK_FR": ExperimentInfo(detock_color_scheme, detock_modules, detock_columns_to_collapse),
    "Detock (Full)": ExperimentInfo(detock_color_scheme, detock_modules, detock_columns_to_collapse),
    "DETOCK_PESSIMISTIC": ExperimentInfo(detock_color_scheme, detock_modules, detock_columns_to_collapse),

    "SLOG": ExperimentInfo(slog_color_scheme, slog_modules, detock_columns_to_collapse),
    "SLOG_FR": ExperimentInfo(slog_color_scheme, slog_modules, detock_columns_to_collapse),

    "Tiga": ExperimentInfo(glog_color_scheme, tiga_modules, tiga_columns_to_collapse),
    "TIGA": ExperimentInfo(glog_color_scheme, tiga_modules, tiga_columns_to_collapse),

}