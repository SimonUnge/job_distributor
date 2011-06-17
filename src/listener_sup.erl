-module (listener_sup).
-include ("couchbeam.hrl").

-export ([start/3, start/1]).

%% Name:
%% Pre :
%% Post:

start(Host, Port, Name) ->
  application:start(sasl),
  application:start(ibrowse),
  application:start(crypto),
  application:start(couchbeam),
  Server = couchbeam:server_connection(Host, Port),
  {ok, Db} = couchbeam:open_or_create_db(Server, Name),
  WorkManagerPid = start_workmanager(Db), 
  ListenerPid = start_listener(Db, WorkManagerPid),%I do not use workmanagerpid, do I?
  receive_loop(WorkManagerPid, ListenerPid, Db).

%%Just for testing purposes.
start(Database) when is_atom(Database)->
  case Database of
    reg_a ->
      start("localhost", 5002, "reg_a");
    reg_b ->
      start("localhost", 5003, "reg_b");
    global_node ->
      start("localhost", 5001, "global_node")
  end.

%% Name:
%% Pre :
%% Post:

receive_loop(WorkManagerPid, ListenerPid, Db) ->
  receive
    {'EXIT', WorkManagerPid, Reason} ->
      io:format("WM ~p died, reason ~p. Restarts.~n", [WorkManagerPid, Reason]),
      NewWorkManagerPid = start_workmanager(Db),
      ListenerPid ! {workmanagerpid, NewWorkManagerPid},
      receive_loop(NewWorkManagerPid, ListenerPid, Db);
    {'EXIT', ListenerPid, Reason} ->
      io:format("Listener ~p died, reason ~p. Restarts.~n", [ListenerPid, Reason]),
      NewListenerPid = start_listener(Db, WorkManagerPid),
      receive_loop(WorkManagerPid, NewListenerPid, Db)
  end.

%% Name:
%% Pre :
%% Post:

start_workmanager(Db) ->
  process_flag(trap_exit, true),
  WorkManagerPid = spawn_link(workmanager, work_manager, [2, Db]),
  WorkManagerPid.

%% Name:
%% Pre :
%% Post:

start_listener(Db, WorkManagerPid) ->
  process_flag(trap_exit, true),
  spawn_link(listener, start, [Db, WorkManagerPid]).
  
  