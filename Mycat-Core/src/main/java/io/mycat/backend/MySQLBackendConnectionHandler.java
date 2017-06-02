/*
 * Copyright (c) 2016, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend;

import java.io.IOException;
import java.util.stream.Stream;

import io.mycat.engine.UserSession;
import io.mycat.front.MySQLFrontConnection;
import io.mycat.net2.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.callback.LoginRespCallback;
import io.mycat.mysql.MySQLConnection;
import io.mycat.net2.ConDataBuffer;
import io.mycat.net2.ConnectionException;
import io.mycat.net2.NIOHandler;
/**
 * backend mysql NIO handler (only one for all backend mysql connections)
 * @author wuzhihui
 *
 */
public class MySQLBackendConnectionHandler implements NIOHandler<MySQLBackendConnection> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackendConnectionHandler.class);

    @Override
    public void onConnected(MySQLBackendConnection con) throws IOException {
    	LoginRespCallback loginRepsCallback=new LoginRespCallback(LOGGER,con.getUserCallback());
    	con.setUserCallback(loginRepsCallback);
        // con.asynRead();
    }

    @Override
    public void handleReadEvent(MySQLBackendConnection con) throws IOException{
        MySQLFrontConnection frontCon=(MySQLFrontConnection) con.getAttachement();
        UserSession session=null;
        if (frontCon!=null) {
            session = frontCon.getSession();
        }

    	ConDataBuffer dataBuffer=con.getReadDataBuffer();
        int offset = dataBuffer.readPos(), length = 0, limit = dataBuffer.writingPos(),initOffset=offset;
        if (session!=null) {
            offset += session.getLeftSize();
        }
        byte packetType = -1;
        int pkgStartPos=offset;


   	// 循环收到的报文处理
        //报文处理分两种情况：1.包头没有读完 2.包体没有读完
		while(true)
		{
		    //如果缓存了头部数据，说明上一次读取信息的最后一个mysql包头信息没有读完
			if (session!=null&&session.getHeader()!=null){
                byte[] header=session.getHeader();
                //获取上一次剩余没有读取的包头长度
			    int leftSize=session.getLeftSize();
			    //获取剩余包头数据
                for (int i=0;i<leftSize;i++){
                    header[4-leftSize+i] = dataBuffer.getByte(offset + 4-leftSize+i);
                }
                //拼接包体
                int bodyLength = dataBuffer.getByte(offset) & 0xff;
                bodyLength |= (dataBuffer.getByte(++offset) & 0xff) << 8;
                bodyLength |= (dataBuffer.getByte(++offset) & 0xff) << 16;
                //设置offset
                offset+=leftSize+bodyLength;
                session.setHeader(null);
            }

		    if(!MySQLConnection.validateHeader(offset, limit))
			{
			    //如果offset<limit说明这个数据包最后一个mysql包包头不全
                if (session!=null&&offset<limit) {
                    //计算还有多少字节包头没有读取
                    int left = 4 + offset-limit;
                    session.setLeftSize(left);
                    //缓存包头
                    byte[] header=new byte[4];
                    for (int i=0;i<left;i++){
                        header[i]=dataBuffer.getByte(offset+i);
                    }
                    session.setHeader(header);
                }
                //还原到初始的offset
				dataBuffer.setReadingPos(initOffset);
				break;
			}

			length = MySQLConnection.getPacketLength(dataBuffer, offset);

			//如果包体部分没有读完，计算下次读取后，offset位移的字节数
			if(length+offset>limit)
			{
				if (session!=null) {
				    //包体没有读完的长度
                    int left = length + offset - limit;
                    session.setLeftSize(left);
                }
                //还原到初始的offset
			    dataBuffer.setReadingPos(initOffset);
				LOGGER.info("Not whole package :length "+length+" cur total length "+limit);
				break;
			}
			// 解析报文类型
            packetType = dataBuffer.getByte(offset+MySQLConnection.msyql_packetHeaderSize);

            pkgStartPos=offset;

			LOGGER.info("received pkg ,length "+length+" type "+packetType+" cur total length "+limit);
			//con.getUserCallback().handleResponse(con, dataBuffer, packetType, pkgStartPos, length);
			offset += length;
			dataBuffer.setReadingPos(offset);
            con.setNextStatus(packetType);
        }

        if (con.getState()== Connection.STATE_IDLE){
            if (session!=null) {
                session.setLeftSize(0);
                session.setHeader(null);
            }
        }

        con.getUserCallback().handleResponse(con, dataBuffer, packetType, pkgStartPos, length);
    }



    @Override
    public void onClosed(MySQLBackendConnection source, String reason) {
    	source.getUserCallback().connectionClose(source, reason);
    }



	@Override
	public void onConnectFailed(MySQLBackendConnection con, ConnectionException e) {
		con.getUserCallback().connectionError(e, con);

	}

	@Override
	public void onHandlerError(MySQLBackendConnection con, Exception e) {
		con.getUserCallback().handlerError( e,con);

	}

  }
