"""Explicit test-only downgrade of empty-metadata legacy311 state to schema3.

Never silently discard MQTT5 properties or finite deadlines to manufacture a legacy
fixture. Production migrations are owned by mqtt-session, not this test helper.
"""
import copy


def schema4_fixture(value):
    result = copy.deepcopy(value)
    if result['schema_version'] == 4:
        return result
    assert result['schema_version'] == 5
    result['schema_version'] = 4
    floor = result.pop('clock_floor')
    assert type(floor) is int and 0 <= floor <= 0xffffffffffffffff

    def old_properties(properties):
        for prop in properties:
            assert 'MessageExpiryInterval' not in prop
            assert 'WillDelayInterval' not in prop

    for session in result['sessions']:
        incoming = []
        for identifier, data, properties, deadline in session['incoming']:
            assert deadline is None
            old_properties(properties)
            incoming.append([identifier, data, properties])
        session['incoming'] = incoming
        for outgoing in session['outgoing']:
            assert outgoing.pop('expires_at') is None
            assert outgoing.pop('started') is True
            old_properties(outgoing['properties'])
        for message in session['queued']:
            assert message.pop('expires_at') is None
            old_properties(message['properties'])
    for retained in result['retained']:
        assert retained.pop('expires_at') is None
        old_properties(retained['properties'])
    for will in result['wills']:
        assert will.pop('delay_interval') == 0
        due = will.pop('due_at')
        assert (due is not None) == will['pending']
        old_properties(will['will']['properties'])
    return result


def schema3_fixture(value):
    result = schema4_fixture(value)
    assert result['schema_version'] == 4
    result['schema_version'] = 3
    for session in result['sessions']:
        assert session.pop('session_expiry_interval') == 0xffffffff
        assert session.pop('expires_at') is None
        incoming = []
        for identifier, wire, properties in session['incoming']:
            assert properties == []
            incoming.append([identifier, wire])
        session['incoming'] = incoming
        for outgoing in session['outgoing']:
            assert outgoing.pop('properties') == []
        for message in session['queued']:
            assert message.pop('properties') == []
    for retained in result['retained']:
        assert retained.pop('properties') == []
    for will in result['wills']:
        assert will['will'].pop('properties') == []
    return result
