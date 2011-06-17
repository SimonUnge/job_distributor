-module (listener).

-include ("couchbeam.hrl").

-export ([start/2]).

%% Name:
%% Pre :
%% Post:

start(Db, WorkManagerPid) ->
  {ok, ReqId} = couchbeam:changes_wait(Db, self(), [{heartbeat, "5000"}]),
  print("StartRef ~p", [ReqId]),
  get_changes(ReqId, Db, WorkManagerPid).

%%Starts a continuous changes stream, and sends the change notification to WorkManagerPid.

%% Name:
%% Pre :
%% Post:

get_changes(ReqId, Db, WorkManagerPid) ->
  receive
    {workmanagerpid, NewWorkManagerPid} ->
      get_changes(ReqId, Db, NewWorkManagerPid);
    {ReqId, done} ->
      print("listener done?"),
      ok;
    {ReqId, {change, Change}} ->
      print("Listener got change."),
      WorkManagerPid ! {changes, Change, Db},
      get_changes(ReqId, Db, WorkManagerPid);
    {ReqId, {error, E}}->
      print("error ? ~p", [E]);
    {'EXIT', SomePid, Reason} ->
      print("XXXSome pid, ~p, died. Reason: ~p", [SomePid, Reason])
  end.

%%== just to have a nicer fuckning print. Hates io:format

%% Name:
%% Pre :
%% Post:

print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).
