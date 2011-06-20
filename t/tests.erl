-module (tests).
-compile (export_all).
-include ("couchbeam.hrl").

start() ->
  Host = "localhost",
  Port = 5002,
  Name = "reg_a",
  application:start(sasl),
  application:start(ibrowse),
  application:start(crypto),
  application:start(couchbeam),
  Server = couchbeam:server_connection(Host, Port),
  {ok, Db} = couchbeam:open_or_create_db(Server, Name),
  {ok, Doc} = couchbeam:open_doc(Db,"testdoc"),
  JobStepList = get_field("job", Doc),
  DocInfo = #document{db = Db, 
             doc = Doc, 
             doc_id = "testdoc", 
             current_step = get_field("step", Doc),
             job_length = length(JobStepList), 
             job_step_do = lists:nth(get_field("step", Doc)+1, JobStepList),
             job_step_list = JobStepList}.


%%==== Helpers =====
change_worker_status(WorkerPid,Workers, NewStatus) ->
  lists:keyreplace(WorkerPid, 1, Workers, {WorkerPid, NewStatus}).

is_job_complete(DocInfo) ->
  DocInfo#document.job_length < DocInfo#document.current_step + 1.

%Returns true if the job-step is claimed.
is_claimed(DocInfo) ->
  ClaimStatus = get_claim_status(DocInfo),
  null =/= ClaimStatus.

has_winner(DocInfo) ->
  WinnerStatus = get_winner_status(DocInfo),
  WinnerStatus =/= null.

is_winner(DocInfo) ->
  WinnerStatus = get_winner_status(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  print("This is DbId: ~p", [DbId]),
  WinnerStatus =:= list_to_binary(DbId).

is_target_any(DocInfo) ->
  Target = get_target(DocInfo),
  Target =:= "any".

is_target_me(DocInfo) ->
  Target = get_target(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  Target =:= DbId.

has_free_workers(Workers) ->
  case get_free_worker(Workers) of
    false ->
      false;
    _Worker ->
      true
  end.
   

save_doc(DocInfo) ->
  couchbeam:save_doc(DocInfo#document.db, DocInfo#document.doc).

%%===== Getters =====

get_id(Change) ->
  {Doc} = Change,
  {<<"id">>, ID} = lists:keyfind(<<"id">>, 1, Doc),
  binary_to_list(ID).

%? Merge with get_id?
get_do(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"do">>, DO} = lists:keyfind(<<"do">>, 1, Step),
  binary_to_list(DO).

get_target(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"target">>, Target} = lists:keyfind(<<"target">>, 1, CurrentJobStep),
  binary_to_list(Target).

get_field(Field, Doc) ->
  couchbeam_doc:get_value(list_to_binary(Field), Doc).

get_free_worker(Workers) ->
  lists:keyfind(free, 2, Workers).

get_current_job_step(DocInfo) -> %XXX Change order of args?
  lists:nth(DocInfo#document.current_step + 1, DocInfo#document.job_step_list). %Erlang list index starts on 1.

get_claim_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"claimed_by">>, ClaimStatus} = lists:keyfind(<<"claimed_by">>, 1, CurrentJobStep),
  ClaimStatus.

get_winner_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"winner">>, WinnerStatus} = lists:keyfind(<<"winner">>, 1, CurrentJobStep),
  WinnerStatus.

%%==== SETTERS ====
set_claim(DocInfo) ->
  true. %XXX

increment_step(DocInfo) ->
  DocInfo#document{doc = set_key_on_doc(DocInfo,
                                        "step",
                                        DocInfo#document.current_step + 1
                                        )
                  }.

set_key_on_doc(DocInfo, Key, Value) ->
  couchbeam_doc:set_value(list_to_binary(Key),
                           Value,
                           DocInfo#document.doc).

%%==== To get job list, change value of key, and return an updated job list, or DocInfo?
update_job_step_list(DocInfo, Key, Value) ->
  {CurrJobStep} = get_current_job_step(DocInfo),
  UpdatedCurrJobStep = lists:keyreplace(list_to_binary(Key), 
                                        1, 
                                        CurrJobStep, 
                                        {list_to_binary(Key), 
                                        list_to_binary(Value)}
                                       ),
  Updated_Job_List = setnth(DocInfo#document.current_step + 1, %THis I hate, + 1, XXX
                            DocInfo#document.job_step_list, 
                            {UpdatedCurrJobStep}
                           ),
  DocInfo#document{job_step_list = Updated_Job_List,
                   doc = set_key_on_doc(DocInfo, "job", Updated_Job_List)}.
  

%% @spec setnth(Index, List, Element) -> list()
%% @doc Replaces element on index Index with Element
setnth(1, [_|Rest], New) -> [New|Rest];
setnth(I, [E|Rest], New) -> [E|setnth(I-1, Rest, New)].

do_work(Do, Retries) ->
  P = open_port({spawn, Do}, [exit_status]),
  case get_status(P) of
    {exit_status, Status} when Status =:= 0 ->
      Status;
    {exit_status, _Status} when Retries > 0 ->
      do_work(Do, Retries - 1);
    {exit_status, Status} ->
      Status
  end. 

get_status(P) ->
  receive
    {P, {exit_status, Status}} ->
      {exit_status, Status};
    {P, Any} ->
      io:format("This is what I got: ~p ~n", [Any]),
      get_status(P)
  end.

%%==== just to have a nicer fuckning print. Hates io:format ====
print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).
