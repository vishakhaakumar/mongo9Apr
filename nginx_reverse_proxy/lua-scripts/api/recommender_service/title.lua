local _M = {}

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.GetTitle()
 local ngx = ngx
 local MovieInfoServiceClient = require "movies_MovieInfoService"
 local GenericObjectPool = require "GenericObjectPool"
  -- Read the parameters sent by the end user client
  
  ngx.req.read_body()
        local post = ngx.req.get_post_args()

        if (_StrIsEmpty(post.movie_name) ) then
           ngx.status = ngx.HTTP_BAD_REQUEST
           ngx.say("Incomplete arguments")
           ngx.log(ngx.ERR, "Incomplete arguments")
           ngx.exit(ngx.HTTP_BAD_REQUEST)
        end

  ngx.say("Inside Nginx Lua script: Processing Get Movie list... Request from: ", post.movie_name)
local client = GenericObjectPool:connection(RecommenderServiceClient, "movie-info-service", 9093)  
 ngx.say("Status: ")

end

return _M
