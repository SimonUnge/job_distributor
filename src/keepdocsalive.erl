-module (keepdocsalive).
-export ([start/1]).
-include("couchbeam.hrl").

%% Name:
%% Pre :
%% Post: 

start(Db) ->
  {_,_,DbId,_} = Db,
  case exist_view_doc(Db, "jobdoc") of
    {true, view_doc_exists} ->
      {ok, ViewObj} = couchbeam:view(Db, {"jobdoc", "unclaimed"}, [{key, list_to_atom(DbId)}]),
      keep_docs_alive(ViewObj, Db);
    {false, no_view_doc} ->
      create_view_doc(Db),
      {ok, ViewObj} = couchbeam:view(Db, {"jobdoc", "unclaimed"}, [{key, list_to_atom(DbId)}]),
      keep_docs_alive(ViewObj, Db)
  end.

%% Name:
%% Pre :
%% Post: 

exist_view_doc(Db, DocId) ->
  case couchbeam:open_doc(Db, DocId) of
    {ok, _Doc} ->
      {true, view_doc_exists};
    {error, not_found} ->
      {false, no_view_doc}
  end.

%% Name:
%% Pre :
%% Post: A created and saved design document that fetches all docs with unclaimed steps.

create_view_doc(Db) ->
 DesignDoc = {[
        {<<"_id">>, <<"_design/jobdoc">>},
        {<<"language">>,<<"javascript">>},
        {<<"views">>,
            {[{<<"unclaimed">>,
                {[{<<"map">>,
                    <<"function (doc) {\n
                        var step, job, claimed;\n
                        step = doc.step;\n
                        job = doc.job[step];\n
                        claimed = job.claimed_by;\n
                        if (claimed == null) {\n
                          emit(doc.creator, doc._id);\n
                        }\n
                      }">>
                }]}
            }]}
        }
    ]},
  couchbeam:save_doc(Db, DesignDoc).

%% Name:
%% Pre :
%% Post:

keep_docs_alive(ViewObj, Db) ->
  timer:sleep(5000),
  io:format("I am keep alive ~p, have just sleept 5 sec.~n",[self()]),
  UnclaimedList = get_unclaimed_list(ViewObj),
  re_save_docs(UnclaimedList, Db),
  keep_docs_alive(ViewObj, Db).

%% Name:
%% Pre :
%% Post:

get_unclaimed_list(ViewObj) ->
  {ok, {ViewList}} = couchbeam_view:fetch(ViewObj),
  {<<"rows">>, UnclaimedList} = lists:keyfind(<<"rows">>, 1, ViewList),
  UnclaimedList.

%% Name:
%% Pre :
%% Post:

re_save_docs([], _Db) ->
  ok;  
re_save_docs([{H} | T], Db) ->
  {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, H),
  {ok, Doc} = couchbeam:open_doc(Db, binary_to_list(DocId)),
  io:format("The Doc: ~p~n", [Doc]),
  couchbeam:save_doc(Db, Doc),
  io:format("Okej, now I will resave the document ~p ~n", [DocId]),
  re_save_docs(T, Db).
  