--!strict

local DataStoreService = game:GetService("DataStoreService")
local ServerStorage = game:GetService("ServerStorage")

local PriorityQueue = require(game.ReplicatedStorage:WaitForChild("ReplicatedScripts"):WaitForChild("Libraries").DataStructures.PriorityQueue)
local ScriptProfiler = require(game.ServerScriptService:WaitForChild("DataStores"):WaitForChild("ScriptProfiler"))
local c = require(game.ServerScriptService:WaitForChild("DataStores"):WaitForChild("DataStoreConstants"))

local Queues = {}
local currentRate = {}
local scriptRates = {}

local datastoresAccessAllowed = true

local DataStoreRequestTypes = {
	["GetAsync"] = Enum.DataStoreRequestType.GetAsync,
	["UpdateAsync"] = Enum.DataStoreRequestType.UpdateAsync,
	["SetIncrementAsync"] = Enum.DataStoreRequestType.SetIncrementAsync,
	["GetSortedAsync"] = Enum.DataStoreRequestType.GetSortedAsync,
	["SetIncrementSortedAsync"] = Enum.DataStoreRequestType.SetIncrementSortedAsync,
	--["OnUpdate"] = Enum.DataStoreRequestType.OnUpdate -- OnUpdate Is Depracated
}

-- for each DataStoreRequestType , create a new PriorityQueue with the number of priorities defined in constants
for _, id in pairs(DataStoreRequestTypes) do
	Queues[id] = PriorityQueue(#c.Enums.Priority)
	currentRate[id] = c.NUMBER_OF_MESSAGES_PER_SECOND
end

local function GetRequestTypeName(requestType : Enum.DataStoreRequestType)
	for name, id in pairs(DataStoreRequestTypes) do
		if id == requestType then return name end
	end
	return ""
end

local function AddMessageToQueue(message, requestType : Enum.DataStoreRequestType)
	if not message.priority then message.priority = c.Enums.Priority.Standard end
	local queue = Queues[requestType]
	if queue then 
		queue:Push(message, message.priority)
		--if no profiler exists yet (first ever message) then create a new profiler for the script:
		if not scriptRates[message.script] then scriptRates[message.script] = ScriptProfiler(message.script) end
		--add a new access to DS to the profiler's counter
		scriptRates[message.script]:NewDSAccess()
	end
end


local function ManageRatePerRequestType(requestType : Enum.DataStoreRequestType)
	local budget = DataStoreService:GetRequestBudgetForRequestType(requestType) --TODO: can this fail? need pcall?
	--warn("BUDGET FOR "..GetRequestTypeName(requestType).." IS "..tostring(budget))
	if currentRate[requestType] == c.NUMBER_OF_MESSAGES_PER_SECOND then --currently regular rate
		--check if need to reduce rate and by how much
		if budget <= c.CRITICAL_BUDGET_RESERVE then
			currentRate[requestType] = c.NUMBER_OF_MESSAGES_PER_SECOND_WHEN_CRITICAL_BUDGET
			warn("Dropped to critical range for queue "..GetRequestTypeName(requestType).." (Current Budget: "..budget..")")
		elseif budget <= c.MINIMUM_BUDGET_RESERVE then
			currentRate[requestType] = c.NUMBER_OF_MESSAGES_PER_SECOND_WHEN_UNDER_BUDGET
			warn("Dropped to reduced range for queue "..GetRequestTypeName(requestType).." (Current Budget: "..budget..")")
		end
	elseif currentRate[requestType] == c.NUMBER_OF_MESSAGES_PER_SECOND_WHEN_UNDER_BUDGET then --currently reduced rate
		--check if need to reduce rate:
		if budget <= c.CRITICAL_BUDGET_RESERVE then 
			currentRate[requestType] = c.NUMBER_OF_MESSAGES_PER_SECOND_WHEN_CRITICAL_BUDGET
			warn("Dropped to critical range for queue "..GetRequestTypeName(requestType).." (Current Budget: "..budget..")")
		--check if need to increase rate:
		elseif budget > c.RETURNING_FROM_MINIMUM_BUDGET then
			currentRate[requestType] = c.NUMBER_OF_MESSAGES_PER_SECOND
			warn("Returned from reduced range to normal range for queue "..GetRequestTypeName(requestType).." (Current Budget: "..budget..")")
		end
	elseif currentRate[requestType] == c.NUMBER_OF_MESSAGES_PER_SECOND_WHEN_CRITICAL_BUDGET then --currently critical rate
		--check if need to increase rate and by how much
		if budget > c.RETURNING_FROM_MINIMUM_BUDGET then
			currentRate[requestType] = c.NUMBER_OF_MESSAGES_PER_SECOND
			warn("Returned from reduced range to normal range for queue "..GetRequestTypeName(requestType).." (Current Budget: "..budget..")")
		elseif budget > c.RETURNING_FROM_CRITICIAL_BUDGET then
			currentRate[requestType] = c.NUMBER_OF_MESSAGES_PER_SECOND_WHEN_UNDER_BUDGET		
			warn("Returned from critical range to reduced range for queue "..GetRequestTypeName(requestType).." (Current Budget: "..budget..")")
		end
	end
	return currentRate[requestType]
end


--wrapper function to retry a function maxAttempts times
local function Retry(func : () -> (),  maxAttempts : number?)
	local attempts = 0
	local success = false
	local result = nil
	local maxAttempts = if maxAttempts then maxAttempts else c.NUMBER_OF_ATTEMPTS_TO_ACCESS_DS
	repeat
		success, result = pcall(func)
		attempts+= 1
	until success or attempts >= maxAttempts
	return success, result
end


local function SendMessageAccordingToRequestType(message, requestType : Enum.DataStoreRequestType)
	local script = message.script
	local params = message.params
	local event = message.event
	local datastore = if message.isSorted then DataStoreService:GetOrderedDataStore(message.dataStore) else DataStoreService:GetDataStore(message.dataStore)
	
	--print("Sending", requestType,  "message from", script, "to datastore ", message.dataStore, "with params:")
	--print(params)
	
	local function HandleSetIncrementAsync()
		if not params or not params[1] then return end
		local setType = params[1]
		table.remove(params, 1)
		if setType == c.Enums.SetType.Set then
			return datastore:SetAsync(table.unpack(params))
		elseif setType == c.Enums.SetType.Increment then
			return datastore:IncrementAsync(table.unpack(params))
		elseif setType == c.Enums.SetType.Remove then
			return datastore:RemoveAsync(table.unpack(params))
		end
	end
	
	local requestTypeMap = {
		[Enum.DataStoreRequestType.GetAsync] = function()
			return datastore:GetAsync(table.unpack(params))
		end,
		--[Enum.DataStoreRequestType.OnUpdate] = function(... : any) --OnUpdate is depracted apparently
		--	return datastore:OnUpdate(...)
		--end,
		[Enum.DataStoreRequestType.UpdateAsync] = function()
			return datastore:UpdateAsync(table.unpack(params))
		end,
		[Enum.DataStoreRequestType.GetSortedAsync] = function()
			return datastore:GetSortedAsync(table.unpack(params)):GetCurrentPage()
		end,
		[Enum.DataStoreRequestType.SetIncrementAsync] = HandleSetIncrementAsync,
		[Enum.DataStoreRequestType.SetIncrementSortedAsync] = HandleSetIncrementAsync
	}
	
	if requestTypeMap[requestType] then
		--print(requestType,":", datastore.Name, ":", datastore.ClassName) --TODO:TEMPOORARY
		local success, result = Retry(requestTypeMap[requestType])
		event:Fire(success, result)
		task.delay(5, function() event:Destroy() end)
	end
end

local finishedLoop = false
local finishedLoopEvent = Instance.new("BindableEvent")

task.defer(function()
	while datastoresAccessAllowed do
		--  for each queue:
		for requestType, queue in pairs(Queues) do
			local ratePerSecond = ManageRatePerRequestType(requestType)
			-- send to the datastores exactly the amount of messages allowed according to the current rate for said requestType:
			ratePerSecond *= c.DATASTORES_ACCESS_RATE
			for i = 1, ratePerSecond do
				local nextMessage = queue:Pop()
				if nextMessage then
					task.spawn(function() SendMessageAccordingToRequestType(nextMessage, requestType) end)
				elseif queue:IsEmpty() then
					break
				else
					warn("Got message = nil, but the queue is not empty!")
				end
			end
		end 
		task.wait(c.DATASTORES_ACCESS_RATE)
	end
	finishedLoop = true
	finishedLoopEvent:Fire()
end)

local function CreateMessageFromParams(scriptName : string , datastoreName : string ,isSorted : boolean, priority : number?, ... : any)
	local message = {}
	message.script = scriptName
	message.dataStore = datastoreName
	message.isSorted = isSorted
	message.priority = if (not priority) or priority < c.Enums.Priority.High or priority > c.Enums.Priority.Low then c.Enums.Priority.Standard else priority
	message.params = {...}
	message.event = Instance.new("BindableEvent")
	return message
end

task.spawn(function() --this is spawned as we want to first set up the listeners and only then instantiate the objects to avoid missed requests 
	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.GetAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName, false , priority, ...)
		if not message then return end
		AddMessageToQueue(message, Enum.DataStoreRequestType.GetAsync)
		return message.event.Event
	end
	
	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.RemoveAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,false, priority, ...)
		if not message then return end
		table.insert(message.params, 1, c.Enums.SetType.Remove)
		AddMessageToQueue(message, Enum.DataStoreRequestType.SetIncrementAsync)
		return message.event.Event
	end
	
	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.RemoveSortedAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName, true,  priority, ...)
		if not message then return end
		table.insert(message.params, 1, c.Enums.SetType.Remove)
		AddMessageToQueue(message, Enum.DataStoreRequestType.SetIncrementSortedAsync)
		return message.event.Event
	end

	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.UpdateAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,false, priority, ...)
		if not message then return end
		AddMessageToQueue(message, Enum.DataStoreRequestType.UpdateAsync)
		return message.event.Event
	end
	
	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.UpdateSortedAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,true, priority, ...)
		if not message then return end
		AddMessageToQueue(message, Enum.DataStoreRequestType.UpdateAsync)
		return message.event.Event
	end

	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.GetSortedAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,true, priority, ...)
		if not message then return end
		AddMessageToQueue(message, Enum.DataStoreRequestType.GetSortedAsync)
		return message.event.Event
	end
	
	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.GetSortedAsyncByKey).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,true, priority, ...)
		if not message then return end
		AddMessageToQueue(message, Enum.DataStoreRequestType.GetAsync)
		return message.event.Event
	end

	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.SetAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,false, priority, ...)
		if not message then return end
		table.insert(message.params, 1, c.Enums.SetType.Set)
		AddMessageToQueue(message, Enum.DataStoreRequestType.SetIncrementAsync)
		return message.event.Event
	end
	
	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.SetSortedAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,true, priority, ...)
		if not message then return end
		table.insert(message.params, 1, c.Enums.SetType.Set)
		AddMessageToQueue(message, Enum.DataStoreRequestType.SetIncrementSortedAsync)
		return message.event.Event
	end

	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.IncrementAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,false, priority, ...)
		if not message then return end
		table.insert(message.params, 1, c.Enums.SetType.Increment)
		AddMessageToQueue(message, Enum.DataStoreRequestType.SetIncrementAsync)
		return message.event.Event
	end
	
	game.ServerScriptService.DataStores.Calls:WaitForChild(c.Enums.Call.IncrementSortedAsync).OnInvoke = function(scriptName : string , datastoreName : string , priority : number?, ... : any)
		if not scriptName or not datastoreName then return warn("wrong params were provided, did you forget to pass script's or datastore's name?") end
		local message = CreateMessageFromParams(scriptName, datastoreName,true, priority, ...)
		if not message then return end
		table.insert(message.params, 1, c.Enums.SetType.Increment)
		AddMessageToQueue(message, Enum.DataStoreRequestType.SetIncrementSortedAsync)
		return message.event.Event
	end
end)


local function CreateBindableFunction(call : string)
	local bindableFunction = Instance.new("BindableFunction")
	bindableFunction.Name = call
	bindableFunction.Parent = game.ServerScriptService.DataStores.Calls
end


CreateBindableFunction(c.Enums.Call.GetAsync)
CreateBindableFunction(c.Enums.Call.SetAsync)
CreateBindableFunction(c.Enums.Call.IncrementAsync)
CreateBindableFunction(c.Enums.Call.UpdateAsync)
CreateBindableFunction(c.Enums.Call.RemoveAsync)
CreateBindableFunction(c.Enums.Call.GetSortedAsync)
CreateBindableFunction(c.Enums.Call.GetSortedAsyncByKey)
CreateBindableFunction(c.Enums.Call.SetSortedAsync)
CreateBindableFunction(c.Enums.Call.IncrementSortedAsync)
CreateBindableFunction(c.Enums.Call.UpdateSortedAsync)
CreateBindableFunction(c.Enums.Call.RemoveSortedAsync)



game:BindToClose(function() --release the queue by sending all the messages when the server shuts down
	task.wait(5) --give some time for other scripts to execute their BindToClose first so maybe they can push a few more messages into the queue
	datastoresAccessAllowed = false --disable the loop that traverses the queue
	if not finishedLoop then finishedLoopEvent.Event:Wait() end
	for requestType, queue in pairs(Queues) do
		while not queue:IsEmpty() do
			local nextMessage = queue:Pop()
			if nextMessage then
				SendMessageAccordingToRequestType(nextMessage, requestType)
			end
		end
	end 
end)

script:SetAttribute("IsReady", true)
