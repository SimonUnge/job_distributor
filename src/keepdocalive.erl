-module (keepdocalive).
-export ([keep_doc_alive/1]).
-include("couchbeam.hrl").


keep_doc_alive(DocInfo) ->
  io:format("IM KEEP ALIVE; DO I EVEN GET HERE~n"),
  io:format("keep alive ~p got a mission, save doc id: ~p ~n",[self(), DocInfo#document.doc_id]),
  timer:sleep(5000),
  utils:save_doc(DocInfo).
