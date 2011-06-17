
-module (workmanager).
-export ([work_manager/2]).
-include("couchbeam.hrl").
  
%% Name:
%% Pre :
%% Post:

work_manager(N, Db) ->
  Workers = start_workers(N, []),
  KeepDocsAliveWorkerPid = start_keep_docs_alive(Db), 
  print("Length workers ~p", [length(Workers)]),
  work_manager_loop(Workers, KeepDocsAliveWorkerPid, Db).
  
%% Name:
%% Pre :
%% Post:

start_workers(N, Workers) when N>0 ->
  process_flag(trap_exit, true),
  WorkerPid = spawn_link(worker, worker, []),
  start_workers(N - 1, [{WorkerPid, free, null} | Workers]);
start_workers(0, Workers) ->
  Workers.
  
%% Name:
%% Pre :
%% Post:

start_keep_docs_alive(Db) ->
  process_flag(trap_exit, true),
  spawn_link(keepdocsalive, start, [Db]).
  
%% Name:
%% Pre :
%% Post:

work_manager_loop(Workers, KeepDocsAliveWorkerPid, Db) ->
  receive
    {status, WorkerPid, DocInfo, {ExecTime, Status}} ->
      case Status of
        {step_status, {do_status, 0}, {alt_do_status, null}} ->
          print("Status from worker ~p: ~p, on doc: ~p",[WorkerPid, Status, DocInfo#document.doc_id]),
          UpdDocInfo1 = utils:set_step_finish_time(DocInfo),
          UpdDocInfo2 = utils:set_job_step_execution_time(UpdDocInfo1, ExecTime/1000000),
          UpdDocInfo3 = utils:set_job_step_status(UpdDocInfo2, "Finished"),
          NewWorkers = utils:change_worker_status(WorkerPid, Workers, free, null),
          print("freed worker ~p, worker list is now: ~p",[WorkerPid, NewWorkers]),
          UpdDocInfo4 = utils:increment_step(UpdDocInfo3),
          print("Saving complete job step for doc: ~p", [UpdDocInfo4#document.doc_id]),
          UpdDocInfo5 = utils:set_job_complete(UpdDocInfo4),
          utils:save_doc(UpdDocInfo5),
          work_manager_loop(NewWorkers, KeepDocsAliveWorkerPid, Db);
        {step_status, {do_status, DoStatus}, {alt_do_status, null}} ->
          StepStatusString = "Failed, no alt, do status: " ++ integer_to_list(DoStatus),
          UpdDocInfo1 = utils:set_step_finish_time(DocInfo),
          UpdDocInfo2 = utils:set_job_step_execution_time(UpdDocInfo1, ExecTime/1000000),
          UpdDocInfo3 = utils:set_job_step_status(UpdDocInfo2, StepStatusString),
          NewWorkers = utils:change_worker_status(WorkerPid, Workers, free, null),
          UpdatedDocInfo = utils:job_step_failed(UpdDocInfo3),
          utils:save_doc(UpdatedDocInfo),
          work_manager_loop(NewWorkers, KeepDocsAliveWorkerPid, Db);
        {step_status, {do_status, DoStatus}, {alt_do_status, AltStatus}} ->
          print("This is where I go when do failed, alt existed"),
          StepStatusString = "Failed, did alt with exit status" ++ integer_to_list(AltStatus),
          UpdDocInfo1 = utils:set_step_finish_time(DocInfo),
          UpdDocInfo2 = utils:set_job_step_execution_time(UpdDocInfo1, ExecTime/1000000),
          UpdDocInfo3 = utils:set_job_step_status(UpdDocInfo2, StepStatusString),
          NewWorkers = utils:change_worker_status(WorkerPid, Workers, free, null),
          UpdatedDocInfo = utils:job_step_failed(UpdDocInfo3),
          utils:save_doc(UpdatedDocInfo),
          work_manager_loop(NewWorkers, KeepDocsAliveWorkerPid, Db)      
      end;
    {'EXIT', KeepDocsAliveWorkerPid, Reason} ->
      print("Keep docs alive proc died, Reason: ~p", [Reason]),
      NewKeepDocsAliveWorkerPid = start_keep_docs_alive(Db),
      work_manager_loop(Workers, NewKeepDocsAliveWorkerPid, Db);
    {'EXIT', WorkerPid, Reason} ->
      UpdWorkers = handle_crashed_worker(WorkerPid, Workers, Db),
      work_manager_loop(UpdWorkers, KeepDocsAliveWorkerPid, Db);
    {changes, Change, Db} ->
      DocId = utils:get_id(Change),
      print("WM got change for doc_id: ~p,~n Will try to open.",[DocId]),
      %%This part is now needed in new version of couchdb that came with design-filter.
      case is_design_document(DocId) of
        false ->
          UpdWorkers = handle_document(Db, DocId, Workers),
          work_manager_loop(UpdWorkers, KeepDocsAliveWorkerPid, Db);
        true ->
          print("a change in designdoc. Ignoring"),
          work_manager_loop(Workers, KeepDocsAliveWorkerPid, Db)
      end
  end.

%% Name:
%% Pre :
%% Post:

handle_document(Db, DocId, Workers) ->
  case couchbeam:open_doc(Db, DocId) of
    {ok, Doc} ->
      print("Doc was opened!"),
      CurrentStepNumber = utils:get_field("step", Doc),
      print("The current step: ~p", [CurrentStepNumber]),
      DocInfo = init_docinfo(Db, Doc, DocId),
      print("Worker status list before handle job: ~p", [Workers]),
      UpdatedWorkers = handle_job(Workers, DocInfo),
      print("And the worker status list after handle job, before loop:~p", [UpdatedWorkers]),
      UpdatedWorkers;
    {error, not_found} ->
      print("Doc deleted?..."),
      Workers
  end.
  
%% Name:
%% Pre :
%% Post:

init_docinfo(Db, Doc, DocId) ->
  JobStepList = utils:get_field("job", Doc),
  #document{
    db = Db,
    doc = Doc,
    doc_id = DocId,
    current_step = utils:get_field("step", Doc),
    job_step_list = JobStepList,
    job_length = length(JobStepList)
  }.
  
%% Name:
%% Pre :
%% Post:
  

handle_job(Workers, DocInfo) ->
  case is_job_complete(DocInfo) of
    false -> %There are still steps that have not been executed.
      print("job is not complete, inspecting winner"),
      inspect_step_and_handle(Workers, DocInfo);
    true ->
      print("Job done"),
      release_worker(Workers, DocInfo)
  end.
  
%% Name:
%% Pre :
%% Post:

inspect_step_and_handle(Workers, DocInfo) ->
  case is_step_executing(DocInfo) of
    true ->
      print("Job step is already running"),
      %XXX Kolla om JAG är den som kör, och om jag verkligen gör det.
      handle_step_is_running(Workers, DocInfo);
    false ->
      print("job is not running, inspect winner and handle:"),
      inspect_winner_and_handle(Workers, DocInfo)
  end.
  
%% Name:
%% Pre :
%% Post:

handle_step_is_running(Workers, DocInfo) ->
  case is_winner(DocInfo) of %%lite galet, men det stämmer. XXX?
    true ->
      %%XXX betyder att jag kör jobbet.
      handle_is_executioner(Workers, DocInfo);
    false ->
      Workers
  end.
  
%% Name:
%% Pre :
%% Post:

handle_is_executioner(Workers, DocInfo) ->
  case is_executing(Workers, DocInfo) of
    true ->
      Workers;
    false ->
      UpdDocInfo = remove_claim_and_winner(DocInfo),
      utils:save_doc(UpdDocInfo),
      Workers
  end.
  
%% Name:
%% Pre :
%% Post:

inspect_winner_and_handle(Workers, DocInfo) ->
  case has_winner(DocInfo) of
    true ->
      print("Job have a winner, handle has winner"),
      handle_has_winner(Workers, DocInfo);
    false ->
    %kolla om det är till alla,
      print("Job does not have a winner, inspect and claim"),
      inspect_claim_and_handle(Workers, DocInfo)
  end.
  
%% Name:
%% Pre :
%% Post:

handle_has_winner(Workers, DocInfo) ->
  case is_winner(DocInfo) of
    true ->
      print("I am winner, handle is winner"),
      handle_is_winner(Workers, DocInfo);
    false ->
      %frigör eventuell bokad worker.
      print("I am not winner, release workers"),
      release_worker(Workers, DocInfo)
  end.
  
%% Name:
%% Pre :
%% Post:

handle_is_winner(Workers, DocInfo) ->
  case have_reserved_worker(Workers, DocInfo) of
    true -> 
      print("I am winner, and have a reserved worker for this:
            doc,step,workerlist: ~p, ~p ~n ~p",[DocInfo#document.doc_id,
                                                DocInfo#document.current_step,
                                                Workers]),
      print("Giving job to reserved worker"),
      give_job_to_reserved_worker(Workers, DocInfo);
    false ->
      print("I am winner, but have no worker reserved. Removes claim and winner, resaves doc."),
      UpdatedDocInfo = remove_claim_and_winner(DocInfo),
      utils:save_doc(UpdatedDocInfo),
      Workers
  end.
  
%% Name:
%% Pre :
%% Post:

inspect_claim_and_handle(Workers, DocInfo) ->
  case is_claimed(DocInfo) of
    true ->
      print("Job is claimed, checkning if I am creator"),
      handle_is_claimed(Workers, DocInfo);
    false ->
      print("Step is not claimed, inspect target and handle..."),
      inspect_target_and_handle(Workers, DocInfo)
  end.
  
%% Name:
%% Pre :
%% Post:

handle_is_claimed(Workers, DocInfo) ->
  case is_job_creator(DocInfo) of
    true ->
      print("I am the creator, setting winner, saves doc, returns workers"),
      UpdDocInfo = utils:set_step_winner(DocInfo),
      utils:save_doc(UpdDocInfo),
      Workers;
    false ->
      print("I am not the creator, returns workers."),
      Workers
  end.
  
%% Name:
%% Pre :
%% Post:

inspect_target_and_handle(Workers, DocInfo) ->
  case is_target_any(DocInfo) of
    true ->
      print("Any is target, handle any target"),
      handle_any_target(Workers, DocInfo);
    false ->
      print("there is a specific target, handle spcigic target"),
      handle_specific_target(Workers, DocInfo)
  end.
  
%% Name:
%% Pre :
%% Post:

handle_any_target(Workers, DocInfo) ->
  case has_free_workers(Workers) of
    true ->
      print("there are free workers, setting claim"),
      UpdatedDocInfo = utils:set_claim(DocInfo),
      UpdatedWorkers = book_worker(Workers, UpdatedDocInfo),
      print("And I have now booked a worker for job:~p ~n Workerlist:~p ",[UpdatedDocInfo#document.doc_id, UpdatedWorkers]),
      utils:save_doc(UpdatedDocInfo),
      UpdatedWorkers;
    false ->
      %XXX Keeping the doc alive by saving it again. But should I increment some value, and then why?
      print("No free workers, doing nothing..."),
      Workers
  end.
  
%% Name:
%% Pre :
%% Post:

handle_specific_target(Workers, DocInfo) ->
  case is_target_me(DocInfo) of
    true ->
      print("I am target, handle me target"),
      handle_me_target(Workers, DocInfo);
    false ->
      print("I am not target, doing nothing..."),
      Workers
  end.
  
%% Name:
%% Pre :
%% Post:

handle_me_target(Workers, DocInfo) ->
  case has_free_workers(Workers) of
    true ->
      print("I am target (so gives work to worker) and has workers, see: ~p", [Workers]),
      UpdDocInfo = utils:set_claim(DocInfo),
      give_job_to_worker(Workers, UpdDocInfo);
    false ->
      print("I am target but have no workers, doing nothing"),
      %XXX Keeping the doc alive by saving it again, in 5 seconds. Creator will resave
      Workers
  end.
%%==== Worker Helpers =====
%% Name: give_job_to_worker
%% Pre : There must exist a free worker
%% Post:: returns worker-list, with one more worker busy.
give_job_to_worker(Workers, DocInfo) ->
  {WorkerPid, free, _DocId} = utils:get_free_worker(Workers), 
  UpdDocInfo = init_docinfo_for_worker(DocInfo, WorkerPid),
  WorkerPid ! {work, self(), UpdDocInfo},
  utils:change_worker_status(WorkerPid, Workers, busy, UpdDocInfo#document.doc_id).

%% Name:
%% Pre :
%% Post:

give_job_to_reserved_worker(Workers, DocInfo) ->
  Doc_Id = DocInfo#document.doc_id,
  case utils:get_reserved_worker(Workers, DocInfo) of
    {WorkerPid, booked, Doc_Id} ->
      UpdDocInfo = init_docinfo_for_worker(DocInfo, WorkerPid),
      WorkerPid ! {work, self(), UpdDocInfo},
      utils:change_worker_status(WorkerPid, Workers, busy, UpdDocInfo#document.doc_id);
    {_WorkerPid, _Status, Doc_Id} ->
      print("for some reason, Im already doing this job..."),
      Workers
  end.

%% Name:
%% Pre :
%% Post:

init_docinfo_for_worker(DocInfo, WorkerPid) ->
  CurrentJobStep = utils:get_current_job_step(DocInfo),
  JobStepDo      = utils:get_do(CurrentJobStep),
  JobStepAltDo   = utils:get_alt_do(CurrentJobStep),
  RetryStrategy  = utils:get_retry_strategy(CurrentJobStep),
  UpdDocInfo1    = DocInfo#document{job_step_do  = JobStepDo,
                                 job_step_alt_do = JobStepAltDo,
                                 retry_strategy  = RetryStrategy},
  UpdDocInfo2 = utils:set_executioner(UpdDocInfo1, WorkerPid),
  UpdDocInfo3 = utils:set_step_start_time(UpdDocInfo2),
  UpdDocInfo4 = utils:set_job_step_status(UpdDocInfo3, "Working"),
  utils:save_doc(UpdDocInfo4).
  
%% Name:
%% Pre :
%% Post:

book_worker(Workers, DocInfo) ->
  {WorkerPid, free, _DocId} = utils:get_free_worker(Workers),
  print("booking worker ~p", [WorkerPid]),
  utils:change_worker_status(WorkerPid, Workers, booked, DocInfo#document.doc_id).
  
%% Name:
%% Pre :
%% Post:

release_worker(Workers, DocInfo) ->
  Doc_Id = DocInfo#document.doc_id,
  case utils:get_reserved_worker(Workers, DocInfo) of
    {WorkerPid, booked, Doc_Id} ->
      print("Release worker: ~p", [WorkerPid]),
      utils:change_worker_status(WorkerPid, Workers, free, null);
    false ->
      print("Ah, I did not book this job"),
      Workers
  end.

handle_crashed_worker(WorkerPid, Workers, Db) ->
  case utils:get_specific_worker(WorkerPid, Workers) of
    {WorkerPid, free, _DocId} ->
      UpdWorkers = lists:keydelete(WorkerPid, 1, Workers),
      NewWorker = create_worker(),
      [{NewWorker, free, null} | UpdWorkers];
    {WorkerPid, booked, DocId} ->
      UpdWorkers = lists:keydelete(WorkerPid, 1, Workers),
      NewWorker = create_worker(),
      [{NewWorker, booked, DocId} | UpdWorkers];
    {WorkerPid, busy, DocId} ->
      UpdWorkers = lists:keydelete(WorkerPid, 1, Workers),
      {ok, Doc} = couchbeam:open_doc(Db, DocId),
      DocInfo = init_docinfo(Db, Doc, DocId),
      UpdDocInfo = utils:job_step_failed(DocInfo),
      utils:save_doc(UpdDocInfo),
      NewWorker = create_worker(),
      [{NewWorker, free, null} | UpdWorkers]
  end.

create_worker() ->
  process_flag(trap_exit, true),
  spawn_link(worker, worker, []).

  
%% Name:
%% Pre :
%% Post:

remove_claim_and_winner(DocInfo) ->
  print("removing my claim"),
  UpdDocInfo1 = utils:update_job_step_list(DocInfo, "claimed_by", null),
  print("...and removing me as winner"),
  UpdDocInfo2 = utils:update_job_step_list(UpdDocInfo1, "winner", null),
  print("and removes eventual executioner"),
  UpdDocInfo3 = utils:update_job_step_list(UpdDocInfo2, "executioner", null),
  print("and step status.... now done?"),
  utils:update_job_step_list(UpdDocInfo3, "step_status", null).
  
%% Name:
%% Pre :
%% Post:

%%=========================  
  
%% Name:
%% Pre :
%% Post:

is_design_document(DocId) ->
  lists:prefix("_design", DocId).
  
%% Name:
%% Pre :
%% Post:

is_job_complete(DocInfo) ->
  DocInfo#document.job_length < DocInfo#document.current_step + 1.
  
%% Name:
%% Pre :
%% Post:

%Returns true if the job-step is claimed.
is_claimed(DocInfo) ->
  ClaimStatus = utils:get_claim_status(DocInfo),
  null =/= ClaimStatus.
  
%% Name:
%% Pre :
%% Post:

is_step_executing(DocInfo) ->
  StepStatus = utils:get_step_status(DocInfo),
  null =/= StepStatus.
  
%% Name:
%% Pre :
%% Post:

has_winner(DocInfo) ->
  WinnerStatus = utils:get_winner_status(DocInfo),
  WinnerStatus =/= null.
  
%% Name:
%% Pre :
%% Post:

is_winner(DocInfo) ->
  WinnerStatus = utils:get_winner_status(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  WinnerStatus =:= list_to_binary(DbId).
  
%% Name:
%% Pre :
%% Post:

is_target_any(DocInfo) ->
  Target = utils:get_target(DocInfo),
  Target =:= "any".
  
%% Name:
%% Pre :
%% Post:

is_target_me(DocInfo) ->
  Target = utils:get_target(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  Target =:= DbId.
  
%% Name:
%% Pre :
%% Post:

is_job_creator(DocInfo) ->
  Creator = utils:get_field("creator", DocInfo#document.doc),
  {_,_,DbId,_} = DocInfo#document.db,
  Creator =:= list_to_binary(DbId).
  
%% Name:
%% Pre :
%% Post:

has_free_workers(Workers) ->
  case utils:get_free_worker(Workers) of
    false ->
      false;
    _Worker ->
      true
  end.
  
%% Name:
%% Pre :
%% Post:

have_reserved_worker(Workers, DocInfo) ->
  case utils:get_reserved_worker(Workers, DocInfo) of
    false ->
      false;
    _Worker ->
      true
  end.
  
%% Name:
%% Pre :
%% Post:

is_executing(Workers, DocInfo) ->
  DocId = DocInfo#document.doc_id,
  case utils:get_busy_worker(Workers, DocInfo) of
    false ->
      false;
    {_WorkerPid, busy, DocId} ->
      true
  end.

%%==== just to have a nicer fuckning print. Hates io:format ====
  
%% Name:
%% Pre :
%% Post:

print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).