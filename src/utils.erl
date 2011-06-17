-module (utils).
-include ("couchbeam.hrl").

-export ([save_doc/1,
          get_id/1,
          get_do/1,
          get_alt_do/1,
          get_target/1,
          get_retry_strategy/1,
          get_field/2,
          get_free_worker/1,
          get_reserved_worker/2,
          get_busy_worker/2,
          get_specific_worker/2,
          get_current_job_step/1,
          get_claim_status/1,
          get_winner_status/1,
          get_step_status/1,
          %setters
          set_claim/1,
          increment_step/1,
          set_key_on_doc/3,
          set_executioner/2,
          set_job_step_status/2,
          set_job_step_execution_time/2,
          set_step_start_time/1,
          set_step_finish_time/1,
          set_job_complete/1,
          change_worker_status/4,
          set_step_winner/1,
          set_step_to_max/1,
          set_job_failed/1,
          job_step_failed/1,
          update_job_step_list/3,
          setnth/3
          ]).

%%Saves the document to the database. If there is a conflict, it does nothing.
  
%% Name:
%% Pre :
%% Post:
save_doc(DocInfo) ->
  case couchbeam:save_doc(DocInfo#document.db, DocInfo#document.doc) of
    {ok, NewDoc} ->
      print("Saving doc id:~p",[DocInfo#document.doc_id]),
      DocInfo#document{ doc = NewDoc };
    {error, conflict} ->
      print("Save conflict on doc:~p",[DocInfo#document.doc_id]),
      DocInfo
  end.

%%==== Document Getters and setters =====

%%Getters
  
%% Name:
%% Pre :
%% Post:

get_id(Change) ->
  {Doc} = Change,
  {<<"id">>, ID} = lists:keyfind(<<"id">>, 1, Doc),
  binary_to_list(ID).
  
%% Name:
%% Pre :
%% Post:

get_do(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"do">>, DO} = lists:keyfind(<<"do">>, 1, Step),
  binary_to_list(DO).
  
%% Name:
%% Pre :
%% Post:

get_alt_do(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"alt_do">>, AltDo} = lists:keyfind(<<"alt_do">>, 1, Step),
  AltDo.
  
%% Name:
%% Pre :
%% Post:

get_target(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"target">>, Target} = lists:keyfind(<<"target">>, 1, CurrentJobStep),
  binary_to_list(Target).
  
%% Name:
%% Pre :
%% Post:

get_retry_strategy(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"retry_strategy">>, {RetryStrategy}} = lists:keyfind(<<"retry_strategy">>, 1, Step),
  RetryStrategy.
  
%% Name:
%% Pre :
%% Post:

get_field(Field, Doc) ->
  couchbeam_doc:get_value(list_to_binary(Field), Doc).
  
%% Name:
%% Pre :
%% Post:

get_free_worker(Workers) ->
  lists:keyfind(free, 2, Workers).
  
%% Name:
%% Pre :
%% Post:

%%Get unfree worker. They both do the same. Logic should be fixed.XXX

%% Name:
%% Pre :
%% Post:

get_reserved_worker(Workers, DocInfo) ->
  lists:keyfind(DocInfo#document.doc_id, 3, Workers).

%% Name:
%% Pre :
%% Post:

get_busy_worker(Workers, DocInfo) ->
  lists:keyfind(DocInfo#document.doc_id, 3, Workers).

%% Name:
%% Pre :
%% Post:

get_specific_worker(Workers, WorkerPid) ->
  lists:keyfind(WorkerPid, 1, Workers).

%% Name:
%% Pre :
%% Post:

get_current_job_step(DocInfo) -> %XXX Change order of args?
  lists:nth(DocInfo#document.current_step + 1, DocInfo#document.job_step_list). %Erlang list index starts on 1.

%% Name:
%% Pre :
%% Post:

get_claim_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"claimed_by">>, ClaimStatus} = lists:keyfind(<<"claimed_by">>, 1, CurrentJobStep),
  ClaimStatus.

%% Name:
%% Pre :
%% Post:

get_winner_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"winner">>, WinnerStatus} = lists:keyfind(<<"winner">>, 1, CurrentJobStep),
  WinnerStatus.

%% Name:
%% Pre :
%% Post:

get_step_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"step_status">>, StepStatus} = lists:keyfind(<<"step_status">>, 1, CurrentJobStep),
  StepStatus.

%% Name:
%% Pre :
%% Post:

%%Setters

set_claim(DocInfo) ->
  {_,_,DbId,_} = DocInfo#document.db,
  update_job_step_list(DocInfo, "claimed_by", list_to_binary(DbId)).

%% Name:
%% Pre :
%% Post:

set_executioner(DocInfo, WorkerPid) ->
  {_,_,DbId,_} = DocInfo#document.db,
  Executioner = "Node: " ++ DbId ++ ", worker: " ++ pid_to_list(WorkerPid),
  update_job_step_list(DocInfo, "executioner", list_to_binary(Executioner)).

%% Name:
%% Pre :
%% Post:

set_job_step_execution_time(DocInfo, Time) ->
  update_job_step_list(DocInfo, "exec_time", Time).

%% Name:
%% Pre :
%% Post:

set_job_step_status(DocInfo, Status) ->
  update_job_step_list(DocInfo, "step_status", list_to_binary(Status)).

%% Name:
%% Pre :
%% Post:

set_step_start_time(DocInfo) ->
  {H, M, S} = time(),
  StartTime = integer_to_list(H) ++ ":" ++ integer_to_list(M) ++ ":" ++ integer_to_list(S),
  update_job_step_list(DocInfo, "start_time", list_to_binary(StartTime)).

%% Name:
%% Pre :
%% Post:

set_step_finish_time(DocInfo) ->
  {H, M, S} = time(),
  FinishTime = integer_to_list(H) ++ ":" ++ integer_to_list(M) ++ ":" ++ integer_to_list(S),
  update_job_step_list(DocInfo, "finish_time", list_to_binary(FinishTime)).

%% Name:
%% Pre :
%% Post:

set_step_winner(DocInfo) ->
  Winner = get_claim_status(DocInfo),
  print("XXXXXXXThe winner~p",[Winner]),
  update_job_step_list(DocInfo, "winner", Winner).

%% Name:
%% Pre :
%% Post:

increment_step(DocInfo) ->
  DocInfo#document{doc = set_key_on_doc(DocInfo,
                                        "step",
                                        DocInfo#document.current_step + 1)
                  }.

%% Name:
%% Pre :
%% Post:

change_worker_status(WorkerPid, Workers, NewStatus, DocId) ->
  lists:keyreplace(WorkerPid, 1, Workers, {WorkerPid, NewStatus, DocId}).

%% Name:
%% Pre :
%% Post:

set_step_to_max(DocInfo) ->
  DocInfo#document{doc = set_key_on_doc(DocInfo, "step", DocInfo#document.job_length)}.

set_job_complete(DocInfo) ->
  if
    DocInfo#document.job_length =< DocInfo#document.current_step + 1 ->
      DocInfo#document{doc = set_key_on_doc(DocInfo, "job_status", list_to_binary("Success"))
                      };
    DocInfo#document.job_length > DocInfo#document.current_step + 1 ->
      DocInfo
  end.

%% Name:
%% Pre :
%% Post:

set_job_failed(DocInfo) ->
  DocInfo#document{doc = set_key_on_doc(DocInfo,
                                        "job_status", 
                                        list_to_binary("failed"))
                          }.

%% Name:
%% Pre :
%% Post:

job_step_failed(DocInfo) ->
  UpdDocInfo = set_step_to_max(DocInfo),
  set_job_failed(UpdDocInfo).

%% Name:
%% Pre :
%% Post:

set_key_on_doc(DocInfo, Key, Value) ->
  couchbeam_doc:set_value(list_to_binary(Key),
                           Value,
                           DocInfo#document.doc).

%% Name:
%% Pre :
%% Post:

%%==== To get job list, change value of key, and return an updated job list, or DocInfo?

%% Name:
%% Pre :
%% Post:

update_job_step_list(DocInfo, Key, Value) ->
  {CurrJobStep} = get_current_job_step(DocInfo),
  UpdatedCurrJobStep = lists:keyreplace(list_to_binary(Key),
                                        1,
                                        CurrJobStep,
                                        {list_to_binary(Key),
                                        Value}
                                       ),
  Updated_Job_List = setnth(DocInfo#document.current_step + 1, %THis I hate, + 1, XXX
                            DocInfo#document.job_step_list, 
                            {UpdatedCurrJobStep}
                           ),
  DocInfo#document{job_step_list = Updated_Job_List,
                   doc = set_key_on_doc(DocInfo, "job", Updated_Job_List)}.
  

%% @spec setnth(Index, List, Element) -> list()
%% @doc Replaces element on index Index with Element

%% Name:
%% Pre :
%% Post:

setnth(1, [_|Rest], New) -> [New|Rest];
setnth(I, [E|Rest], New) -> [E|setnth(I-1, Rest, New)].


%%==== just to have a nicer fuckning print. Hates io:format ====

%% Name:
%% Pre :
%% Post:

print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).

