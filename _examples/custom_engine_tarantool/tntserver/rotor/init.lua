#!/usr/bin/env tarantool

--[[
    Устаналвиваем текущую директорию,
    как начальный путь загрузки всех модулей
--]]
package.setsearchroot()

local cartridge = require('cartridge')
local log = require('log')

log.info(cartridge)

--[[
    Конфигурируем и запускаем cartridge на узле
    Указываем какие роли мы будем использовать в кластере
    Указываем рабочую директорию для хранения снапов, икслогов
    и конфигурации приложения `one`
]]
local _, err = cartridge.cfg({
    workdir = 'one', -- default

    roles = {
        'centrifuge',
    },
})
if err ~= nil then
    log.info(err)
    os.exit(1)
end
