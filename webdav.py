import functools
import hashlib
import logging
import os.path
import urllib.parse
from datetime import datetime

from lxml import etree
from quart import Quart, request, Response, abort, stream_with_context

from storage import StorageProvider

logger = logging.getLogger(__name__)


class WebDav:
    ALLOWED_METHODS = tuple((
        'OPTIONS', 'HEAD', 'GET', 'PUT', 'DELETE', 'PROPFIND', 'PROPPATCH', 'MKCOL', 'LOCK', 'UNLOCK', 'MOVE', 'COPY',
    ))

    PROPS = tuple((
        '{DAV:}resourcetype', '{DAV:}displayname',
        '{DAV:}creationdate', '{DAV:}getlastmodified',
        '{DAV:}getcontentlength', '{DAV:}getcontenttype',
        '{DAV:}getetag',
        '{DAV:}supportedlock', '{DAV:}lockdiscovery',
    ))

    def __init__(self, storage: StorageProvider):
        self.storage = storage
        self.app = Quart(__name__)
        self.app.add_url_rule('/<path:path>', view_func=self.request_handler, methods=WebDav.ALLOWED_METHODS)
        self.app.add_url_rule('/', defaults={'path': ''}, view_func=self.request_handler, methods=self.ALLOWED_METHODS)

    @staticmethod
    async def do_options(*_, **__):
        return Response(
            response='',
            status=200, headers={
                'Allow': ', '.join(WebDav.ALLOWED_METHODS),
                'Content-length': 0,
                'DAV': '1, resumable-upload',  # OSX Finder need Ver 2, if Ver 1 -- read only
                'Server': 'WebDAV',
                # 'MS-Author-Via': 'DAV',
            },
        )

    async def request_handler(self, path: str):
        logger.debug('Request: %s %s %s', request.method, path, request.headers)
        if not await self.auth():
            abort(403)
        handlers = {
            'OPTIONS': self.do_options,
            'HEAD': self.do_head,
            'GET': self.do_get,
            'PUT': self.do_put,
            'DELETE': self.do_delete,
            'MKCOL': self.do_mkcol,
            'PROPFIND': self.do_propfind,
            'PROPPATCH': self.do_proppatch,
            'MOVE': functools.partial(self.do_copy_move, request.method),
            'COPY': functools.partial(self.do_copy_move, request.method),
        }
        handler = handlers.get(request.method)
        if not handler:
            abort(503)

        logger.info('Request: [%s][%s][Range: %s]', request.method, path, request.headers.get('Range'))

        return await handler(path)

    async def auth(self) -> bool:
        return True  # TODO

    async def do_head(self, path: str):
        return await self.do_get(path, meta_only=True)

    async def do_get(self, path: str, *, meta_only=False):
        exists = await self.storage.exists(path)
        if not exists:
            abort(404)
        is_dir = exists and await self.storage.is_dir(path)
        if is_dir:
            abort(403)  # list dir - propfind
        stats = await self.storage.get_meta(path)

        get_range = request.headers.get('Range')
        if get_range:
            start, end = get_range.split('=')[1].split('-')
            start = int(start) if start else 0
            end = int(end) if end else stats.size - 1
        else:
            start, end = 0, stats.size - 1
        content_length = end - start + 1

        headers = {
            'Accept-Ranges': 'bytes',
            'Content-Type': stats.mime_type or 'application/octet-stream',
            'Last-Modified': stats.mtime,
        }
        if meta_only:
            return Response(status=200, headers=headers)

        headers.update({
            'Content-Length': content_length,
            'Content-Range': f'bytes {start}-{end}/{stats.size}',
        })

        if stats.size == 0:
            return Response(status=204, headers=headers)

        status = 200 if content_length == stats.size else 206

        @stream_with_context
        async def stream():
            async for chunk in self.storage.read_file(path, offset=start, length=content_length):
                yield chunk

        return Response(stream(), status=status, headers=headers)

    async def do_mkcol(self, path: str):
        if not path:
            abort(400)
        if await self.storage.exists(path):
            abort(409)
        await self.storage.mkdir(path)
        return Response(status=201)

    async def do_put(self, path: str):
        exists = await self.storage.exists(path)
        is_dir = exists and await self.storage.is_dir(path)
        if is_dir:
            abort(405)  # list dir - propfind

        await self.storage.write_file(path, request.body, 0)
        return Response(status=204 if exists else 201)

    async def do_delete(self, path: str):
        if not path:
            abort(400)
        exists = await self.storage.exists(path)
        if not exists:
            abort(404)
        await self.storage.rm(path)
        return Response(status=204)

    async def do_copy_move(self, method: str, path: str):
        if not path:
            abort(400)
        if not await self.storage.exists(path):
            abort(404)
        destination = request.headers.get('Destination')
        destination = urllib.parse.urlparse(destination)
        destination = destination.path.strip('/')
        destination = urllib.parse.unquote(destination)
        if not destination:
            abort(400)
        overwrite = request.headers.get('Overwrite', 'T').lower()
        if overwrite == 't':
            overwrite = True
        elif overwrite == 'f':
            overwrite = False
        else:
            abort(400)

        if not overwrite and await self.storage.exists(destination):
            abort(412)
        if method == 'COPY':
            await self.storage.cp(path, destination)
        elif method == 'MOVE':
            await self.storage.mv(path, destination)
        else:
            abort(400)
        return Response(status=201)

    async def do_propfind(self, path: str):
        body_str = await request.get_data()
        logger.debug('propfind: body %s', body_str)

        propfind_type = 'allprop'
        requested_props = []

        if body_str:
            body = etree.fromstring(body_str)
            propfind_child = body.find('{DAV:}allprop') or body.find('{DAV:}propname') or body.find('{DAV:}prop')
            if propfind_child is None:
                abort(400)
            propfind_type = propfind_child.tag.split('}')[1]
            if propfind_type == 'prop':
                requested_props = [child.tag for child in propfind_child]

        headers = request.headers
        logger.debug('propfind: headers %s', headers)

        depth = headers.get('Depth')
        if depth not in ('0', '1'):
            abort(400)

        logger.info('propfind: request %s %s %s %s', path, depth, propfind_type, requested_props)

        if not await self.storage.exists(path):
            abort(404)

        ele_multistatus = etree.Element('{DAV:}multistatus')
        resources = [path]
        if depth == '1' and await self.storage.is_dir(path):
            for c in await self.storage.ls(path):
                resources.append(os.path.join(path, c))
        for resource in resources:
            ele_rsp = etree.SubElement(ele_multistatus, '{DAV:}response')
            ele_href = etree.SubElement(ele_rsp, '{DAV:}href')
            ele_href.text = f'/{resource}'

            ele_propstat = etree.SubElement(ele_rsp, '{DAV:}propstat')
            ele_prop = etree.SubElement(ele_propstat, '{DAV:}prop')

            if propfind_type == 'propname':
                for prop in WebDav.PROPS:
                    etree.SubElement(ele_prop, prop)
            else:
                resource_stat = await self.storage.get_meta(resource)
                props = WebDav.PROPS if propfind_type == 'allprop' else requested_props
                for prop in props:
                    if prop == '{DAV:}getlastmodified':
                        etree.SubElement(ele_prop, prop).text = \
                            datetime.utcfromtimestamp(resource_stat.mtime).strftime('%a, %d %b %Y %H:%M:%S GMT')
                    elif prop == '{DAV:}creationdate':
                        etree.SubElement(ele_prop, prop).text = \
                            datetime.utcfromtimestamp(resource_stat.ctime).strftime('%a, %d %b %Y %H:%M:%S GMT')
                    elif prop == '{DAV:}getcontentlength':
                        if not resource_stat.is_dir:
                            etree.SubElement(ele_prop, prop).text = str(resource_stat.size)
                    elif prop == '{DAV:}getcontenttype':
                        if not resource_stat.is_dir:
                            etree.SubElement(ele_prop, prop).text = resource_stat.mime_type
                    elif prop == '{DAV:}resourcetype':
                        ele_res_type = etree.SubElement(ele_prop, prop)
                        if resource_stat.is_dir:
                            etree.SubElement(ele_res_type, '{DAV:}collection')
                    elif prop == '{DAV:}displayname':
                        etree.SubElement(ele_prop, prop).text = resource_stat.name
                    elif prop == '{DAV:}lockdiscovery':
                        pass  # TODO
                    elif prop == '{DAV:}supportedlock':
                        pass  # TODO
                    elif prop == '{DAV:}getetag':
                        etree.SubElement(ele_prop, prop).text = \
                            hashlib.md5(f'{int(resource_stat.mtime)}-{resource_stat.size}'.encode('utf-8')).hexdigest()
                    else:
                        logger.warning('propfind: unknown prop %s', prop)
                # end for prop in props
            logger.debug('propfind: %s %s', resource, ele_prop)
            ele_status = etree.SubElement(ele_propstat, '{DAV:}status')
            ele_status.text = 'HTTP/1.1 200 OK'

        return Response(
            etree.tostring(ele_multistatus, encoding='utf-8', method='xml'),
            status=207, content_type='application/xml; charset=utf-8',
        )

    async def do_proppatch(self, path: str):
        pass  # TODO
