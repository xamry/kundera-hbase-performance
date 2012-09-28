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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import com.impetus.kunderahbase.dao.KunderaBaseDao;
import com.impetus.kunderahbase.dto.UserHBaseDTO;

/**
 * @author amresh.singh
 * 
 */
public class UserDaoKunderaImpl extends KunderaBaseDao implements UserDao
{

    @Override
    public void init()
    {
        startup();
    }

    @Override
    public void insertUsers(List<UserHBaseDTO> users, boolean isBulk)
    {

        if (isBulk)
        {
            EntityManager em = emf.createEntityManager();

            for (int i = 0; i < users.size(); i++)
            {
                UserHBaseDTO user = users.get(i);
                if (i % 4000 == 0)
                {
                    em.clear();
                }
                em.persist(user);
            }
            em.close();
            em = null;
        }
        else
        {
            for (int i = 0; i < users.size(); i++)
            {
                UserHBaseDTO user = users.get(i);
                insertUser(user);
            }
        }
        users.clear();
        users = null;
    }

    /**
     * Inserts user table object.
     * 
     * @param user
     *            user object.
     */
    public void insertUser(UserHBaseDTO user)
    {
        EntityManager em = emf.createEntityManager();
        em.persist(user);
        em.close();
        em = null;
    }

    @Override
    public void updateUser(UserHBaseDTO userDTO)
    {
    }

    @Override
    public void deleteUser(String userId)
    {
    }

    @Override
    public void cleanup()
    {
        System.out.println("<<<<<<< Shututdown called>>>>>");
        shutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kunderahbase.dao.user.UserDao#findUserById(boolean,
     * java.util.List)
     */
    @Override
    public void findUserById(boolean isBulk, List<UserHBaseDTO> users)
    {
        EntityManager em = null;
        if (isBulk)
        {
            em = emf.createEntityManager();
        }

        for (UserHBaseDTO u : users)
        {
            if (!isBulk)
            {
                em = emf.createEntityManager();
            }

            UserHBaseDTO result = em.find(UserHBaseDTO.class, u.getUserId());
            assert result != null;
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kunderahbase.dao.user.UserDao#findUserByUserName(java.lang
     * .String, boolean, java.util.List)
     */
    @Override
    public void findUserByUserName(String userName, boolean isBulk, List<UserHBaseDTO> users)
    {
        EntityManager em = null;
        if (isBulk)
        {
            em = emf.createEntityManager();
        }

        String sql = "Select p from UserHBaseDTO p where p.userNameCounter = ";

        for (UserHBaseDTO u : users)
        {
            if (!isBulk)
            {
                em = emf.createEntityManager();
            }

            Query q = em.createQuery(sql + u.getUserNameCounter());
            assert q.getResultList() != null;
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kunderahbase.dao.user.UserDao#findAll(int)
     */
    @Override
    public void findAll(int count)
    {
        String sql = "Select p from UserHBaseDTO p";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(sql);
        q.setMaxResults(count);
        assert q.getResultList() != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kunderahbase.dao.user.UserDao#findAllByUserName(int)
     */
    @Override
    public void findAllByUserName(int count)
    {
        String sql = "Select p from UserHBaseDTO p where p.userName= Amry";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(sql);

        List<UserHBaseDTO> results = q.getResultList();
        assert results != null && results.size() == count;

    }
}
