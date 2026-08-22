"""Runtime descriptors for BKK's published realCity GTFS-RT extension.

Source schema: https://opendata.bkk.hu/docs/gtfs-realtime-realcity.proto

Keeping the tiny descriptor here avoids a protoc build step in the production
image while still using protobuf's normal extension API.  Raw protobuf bytes
remain authoritative if BKK evolves the extension later.
"""

from __future__ import annotations

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.transit import gtfs_realtime_pb2  # noqa: F401 - registers dependency


_POOL = descriptor_pool.Default()
_FILE_NAME = "gtfs-realtime-realcity.proto"


def _field(message, name, number, field_type, *, label=None, type_name=None, default=None):
    value = message.field.add()
    value.name = name
    value.number = number
    value.type = field_type
    value.label = label or descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    if type_name:
        value.type_name = type_name
    if default is not None:
        value.default_value = default
    return value


def _extension(file_proto, name, number, extendee, type_name):
    value = file_proto.extension.add()
    value.name = name
    value.number = number
    value.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    value.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    value.extendee = extendee
    value.type_name = type_name


def _register() -> None:
    try:
        _POOL.FindFileByName(_FILE_NAME)
        return
    except KeyError:
        pass

    f = descriptor_pb2.FileDescriptorProto()
    f.name = _FILE_NAME
    f.package = "realcity"
    f.syntax = "proto2"
    f.dependency.append("gtfs-realtime.proto")

    vehicle = f.message_type.add()
    vehicle.name = "VehicleDescriptor"
    _field(vehicle, "vehicle_model", 1, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    _field(vehicle, "deviated", 2, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL, default="false")
    _field(vehicle, "vehicle_type", 3, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    _field(vehicle, "door_open", 4, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL)
    _field(vehicle, "stop_distance", 5, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)

    stop_update = f.message_type.add()
    stop_update.name = "StopTimeUpdate"
    event_type = ".transit_realtime.TripUpdate.StopTimeEvent"
    _field(stop_update, "scheduled_arrival", 1, descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, type_name=event_type)
    _field(stop_update, "scheduled_departure", 2, descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, type_name=event_type)

    route = f.message_type.add()
    route.name = "RouteDetail"
    _field(
        route,
        "route_id",
        1,
        descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED,
    )
    _field(route, "header_text", 2, descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, type_name=".transit_realtime.TranslatedString")
    _field(route, "cause", 3, descriptor_pb2.FieldDescriptorProto.TYPE_ENUM, type_name=".transit_realtime.Alert.Cause")
    _field(route, "effect", 4, descriptor_pb2.FieldDescriptorProto.TYPE_ENUM, type_name=".transit_realtime.Alert.Effect")
    effect_type = route.enum_type.add()
    effect_type.name = "EffectType"
    for name, number in (("NO_SERVICE", 1), ("WARNING", 2)):
        enum_value = effect_type.value.add()
        enum_value.name = name
        enum_value.number = number
    _field(route, "effect_type", 5, descriptor_pb2.FieldDescriptorProto.TYPE_ENUM, type_name=".realcity.RouteDetail.EffectType")

    alert = f.message_type.add()
    alert.name = "Alert"
    _field(alert, "startText", 1, descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, type_name=".transit_realtime.TranslatedString")
    _field(alert, "endText", 2, descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, type_name=".transit_realtime.TranslatedString")
    _field(alert, "modifiedTime", 3, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64)
    _field(
        alert,
        "route",
        4,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED,
        type_name=".realcity.RouteDetail",
    )

    _extension(f, "vehicle", 1006, ".transit_realtime.VehicleDescriptor", ".realcity.VehicleDescriptor")
    _extension(f, "stop_time_update", 1006, ".transit_realtime.TripUpdate.StopTimeUpdate", ".realcity.StopTimeUpdate")
    _extension(f, "alert", 1006, ".transit_realtime.Alert", ".realcity.Alert")
    _POOL.AddSerializedFile(f.SerializeToString())


_register()

VehicleDescriptor = message_factory.GetMessageClass(_POOL.FindMessageTypeByName("realcity.VehicleDescriptor"))
StopTimeUpdate = message_factory.GetMessageClass(_POOL.FindMessageTypeByName("realcity.StopTimeUpdate"))
RouteDetail = message_factory.GetMessageClass(_POOL.FindMessageTypeByName("realcity.RouteDetail"))
Alert = message_factory.GetMessageClass(_POOL.FindMessageTypeByName("realcity.Alert"))

vehicle = _POOL.FindExtensionByName("realcity.vehicle")
stop_time_update = _POOL.FindExtensionByName("realcity.stop_time_update")
alert = _POOL.FindExtensionByName("realcity.alert")
