-module (worker).
-export ([worker/0, do_work/2]).
-include("couchbeam.hrl").

%% Name:
%% Pre :
%% Post: 

worker() ->
  receive
    {work, From, DocInfo} ->
      RetryStrategies = retrieve_strategies(DocInfo#document.retry_strategy),

      {ExecTime, Status} = timer:tc(worker, do_work, [DocInfo, RetryStrategies]),

      From ! {status, self(), DocInfo, {ExecTime, Status}},
      worker()
  end.

%% Name:
%% Pre :
%% Post: Return status of do. On failure, retries MaxR times, then alt_do if any.

do_work(DocInfo, {MaxR, MaxT, Sleep, SleepF,SleepM}) ->
  P = open_port({spawn, DocInfo#document.job_step_do}, [exit_status]),
  case get_status(P) of
    {exit_status, Status} when Status =:= 0 ->
      {step_status,{do_status, Status}, {alt_do_status, null}};%%All went well.
    {exit_status, _Status} when MaxR > 0 ->
      print("work failed, retry"),
      NewSleep = sleep_strategy(Sleep, SleepF, SleepM),
      do_work(DocInfo, {MaxR - 1, MaxT, NewSleep, SleepF, SleepM});
    {exit_status, Status} ->
      alternative_do(DocInfo, Status)
      %%alternative do?
  end.

%% Name:
%% Pre :
%% Post: Exit status from shell command.

get_status(P) ->
  receive
    {P, {exit_status, Status}} ->
      {exit_status, Status};
    {P, Any} ->
      print("This is what I got: ~p", [Any]),
      get_status(P)
  end.

%% Name:
%% Pre :
%% Post: Executes the alternative do for the step, if any.

alternative_do(DocInfo, Status) ->
  case DocInfo#document.job_step_alt_do of
    null ->
      {step_status,{do_status, Status}, {alt_do_status, null}};
    AltDo ->
      P = open_port({spawn, binary_to_list(AltDo)}, [exit_status]),
      {exit_status, AltStatus} = get_status(P),
      {step_status, {do_status, Status}, {alt_do_status, AltStatus}}
  end.

%% Name:
%% Pre :
%% Post: Sleeping for sleep seconds, if not null.

sleep_strategy(Sleep, SleepFactor, SleepMax) ->
  case Sleep of
    null ->
      Sleep;
    Integer ->
      print("Worker will now sleep for ~p seconds",[Integer]),
      timer:sleep(Integer * 1000),
      new_sleep(Sleep, SleepFactor, SleepMax)
  end.

%% Name:
%% Pre : Int sleep, int sleepfactor, int sleepmax.
%% Post: int (Sleep * sleepfactor), if less than sleepmax, else sleepmax. 

new_sleep(Sleep, SleepFactor, SleepMax) ->
  NewSleep = Sleep * SleepFactor,
  if
    NewSleep < SleepMax ->
      NewSleep;
    NewSleep >= SleepMax ->
      SleepMax
  end.

%% Name:
%% Pre :
%% Post: Sleep strategies received.

retrieve_strategies(RetryStrategies) ->
  {_, MaxRetries}       = lists:keyfind(<<"max_retries">>,  1, RetryStrategies),
  {_, MaxTime}          = lists:keyfind(<<"max_time">>,     1, RetryStrategies),
  {_, Sleep}            = lists:keyfind(<<"sleep">>,        1, RetryStrategies),
  {_, SleepFactor}      = lists:keyfind(<<"sleep_factor">>, 1, RetryStrategies),
  {_, SleepMax}         = lists:keyfind(<<"sleep_max">>,    1, RetryStrategies),
  {MaxRetries, MaxTime, Sleep, SleepFactor, SleepMax}.

%%== just to have a nicer, quicker, print. Hates io:format

%% Name:
%% Pre :
%% Post:

print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).
  