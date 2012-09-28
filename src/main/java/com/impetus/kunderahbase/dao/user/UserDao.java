/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kunderahbase.dao.user;

import java.util.List;

import com.impetus.kunderahbase.dto.UserHBaseDTO;

/**
 * @author amresh.singh
 * 
 */
public interface UserDao
{

    public void init();

    public void insertUsers(List<UserHBaseDTO> users, boolean isBulk);

    public void insertUser(UserHBaseDTO user);

    public void updateUser(UserHBaseDTO userDTO);

    public void findUserById(boolean isBulk, List<UserHBaseDTO> users);

    public void findUserByUserName(String userName, boolean isBulk, List<UserHBaseDTO> users);

    public void deleteUser(String userId);

    public void cleanup();

    public void findAll(int count);

    public void findAllByUserName(int count);

}
